import os
import subprocess
import tempfile
import shutil
from google.cloud import storage

def check_for_ffmpeg():
    """Checks if FFmpeg and ffprobe are installed and available in the system's PATH."""
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        print("🚨 CRITICAL ERROR: FFmpeg or ffprobe not found.")
        print("Please install FFmpeg and ensure it's in your system's PATH.")
        exit()

def list_blobs_in_folder(bucket_name, folder_name):
    """Lists all blobs in a specific 'folder' in a GCS bucket."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name)
    # Filter out the folder itself and non-video files
    video_extensions = ('.mp4', '.mov', '.avi', '.mkv', '.webm')
    video_blobs = [
        blob for blob in blobs 
        if blob.name != folder_name and blob.name.lower().endswith(video_extensions)
    ]
    # Sort blobs alphabetically to ensure a consistent stitching order
    video_blobs.sort(key=lambda x: x.name)
    return video_blobs

def download_blob(blob, destination_dir):
    """Downloads a blob from GCS to a local directory."""
    file_name = os.path.basename(blob.name)
    destination_path = os.path.join(destination_dir, file_name)
    blob.download_to_filename(destination_path)
    print(f"✅ Downloaded {blob.name} to {destination_path}")
    return destination_path

def upload_blob(bucket_name, source_file_path, destination_blob_name):
    """Uploads a file to a GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"🚀 Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")

def get_video_duration(video_path):
    """Gets the duration of a video in seconds using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", video_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return float(result.stdout)

def stitch_two_videos(video1_path, video2_path, output_path, transition_effect="fade", transition_duration=0.6):
    """Stitches two videos with a specified transition using FFmpeg."""
    duration1 = get_video_duration(video1_path)
    offset = duration1 - transition_duration

    command = [
        "ffmpeg", "-y", # -y overwrites output file if it exists
        "-i", video1_path,
        "-i", video2_path,
        "-filter_complex", (
            f"[0:v][1:v]xfade=transition={transition_effect}:duration={transition_duration}:offset={offset},format=yuv420p[v];"
            f"[0:a][1:a]acrossfade=d={transition_duration}[a]"
        ),
        "-map", "[v]",
        "-map", "[a]",
        "-movflags", "+faststart",
        output_path
    ]
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(f"🎬 Stitched {os.path.basename(video1_path)} and {os.path.basename(video2_path)}")

def main():
    """Main function to orchestrate the download, stitching, and upload process."""
    # --- Configuration ---
    BUCKET_NAME = "veo_exps_prod" 
    SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
    DESTINATION_PREFIX = "CRED_exps/output_videos/"
    FINAL_FILENAME = "final_video_with_xfade_transitions.mp4"
    
    check_for_ffmpeg()

    print(f"Listing videos in gs://{BUCKET_NAME}/{SOURCE_PREFIX}...")
    video_blobs = list_blobs_in_folder(BUCKET_NAME, SOURCE_PREFIX)

    if len(video_blobs) < 2:
        print(f"Error: Found {len(video_blobs)} videos. Need at least 2 to stitch.")
        return

    print(f"Found {len(video_blobs)} videos to stitch.")

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        # Download all videos first
        local_video_paths = [download_blob(blob, temp_dir) for blob in video_blobs]

        # Iteratively stitch the videos
        current_video_path = local_video_paths[0]
        for i in range(1, len(local_video_paths)):
            next_video_path = local_video_paths[i]
            # Define a name for the intermediate stitched video
            intermediate_output_path = os.path.join(temp_dir, f"temp_stitch_{i}.mp4")
            
            stitch_two_videos(current_video_path, next_video_path, intermediate_output_path)
            
            # The output of this iteration becomes the input for the next
            current_video_path = intermediate_output_path

        # The final stitched video is the last `current_video_path`
        final_video_path = current_video_path
        print(f"🎉 Final stitched video created at: {final_video_path}")

        # Upload the final video to GCS
        destination_blob_name = f"{DESTINATION_PREFIX}{FINAL_FILENAME}"
        upload_blob(BUCKET_NAME, final_video_path, destination_blob_name)

    print("✅ Process complete. Temporary files have been deleted.")

if __name__ == "__main__":
    main()