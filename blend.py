import os
import tempfile
import shutil
from moviepy.editor import VideoFileClip, CompositeVideoClip, vfx
from google.cloud import storage

# --- Configuration ---
BUCKET_NAME = "veo_exps_prod" 
SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
DESTINATION_PREFIX = "CRED_exps/output_videos/"
FINAL_VIDEO_NAME = "concatenated_video_blend.mp4"
TRANSITION_DURATION = 1.5 # Duration of the blend in seconds

def process_gcs_videos_with_blend():
    """
    Downloads videos, applies a crossfade/blend transition between them,
    and uploads the result back to GCS.
    """
    local_temp_dir = tempfile.mkdtemp()
    print(f"📁 Created temporary directory: {local_temp_dir}")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # 1. Download all videos from the source GCS folder
        print(f"⬇️  Downloading videos from gs://{BUCKET_NAME}/{SOURCE_PREFIX}...")
        blobs = bucket.list_blobs(prefix=SOURCE_PREFIX)
        downloaded_files = []
        for blob in blobs:
            if not blob.name.lower().endswith(('.mp4', '.mov', '.avi')) or blob.name.endswith('/'):
                continue
            file_name = os.path.basename(blob.name)
            local_path = os.path.join(local_temp_dir, file_name)
            blob.download_to_filename(local_path)
            downloaded_files.append(local_path)
            print(f"   - Downloaded {file_name}")

        if not downloaded_files:
            print("⚠️ No video files found. Exiting.")
            return

        downloaded_files.sort()

        # 2. Apply blend/crossfade transitions
        print("\n🎬 Applying blend (crossfade) transitions...")
        clips = [VideoFileClip(f) for f in downloaded_files]
        
        # The first clip is faded out at its end
        clips_to_stitch = [clips[0].fx(vfx.fadeout, TRANSITION_DURATION)]
        
        # Subsequent clips are faded in and start before the previous clip ends
        for i in range(1, len(clips)):
            clip = clips[i].fx(vfx.fadein, TRANSITION_DURATION)
            # We set the start time of the new clip to be during the fadeout of the previous one
            clips_to_stitch.append(clip.set_start(clips[i-1].duration - TRANSITION_DURATION))
        
        # The last clip shouldn't fade out at the very end
        clips_to_stitch[-1] = clips[-1].fx(vfx.fadein, TRANSITION_DURATION).set_start(clips[-2].duration - TRANSITION_DURATION)

        # Create the final video by compositing the clips.
        # This layers them according to their start times.
        final_clip = CompositeVideoClip(clips_to_stitch)
        final_video_path = os.path.join(local_temp_dir, FINAL_VIDEO_NAME)

        print("\n💾 Writing final video locally...")
        final_clip.write_videofile(final_video_path, codec="libx264", audio_codec="aac")

        # 3. Upload the final video back to GCS
        destination_blob_name = os.path.join(DESTINATION_PREFIX, FINAL_VIDEO_NAME)
        print(f"\n⬆️  Uploading final video to gs://{BUCKET_NAME}/{destination_blob_name}...")
        
        blob_upload = bucket.blob(destination_blob_name)
        blob_upload.upload_from_filename(final_video_path)

        print("✅ Process complete!")

    except Exception as e:
        print(f"❌ An error occurred: {e}")

    finally:
        # 4. Clean up the local temporary directory
        if os.path.exists(local_temp_dir):
            print(f"\n🗑️  Cleaning up temporary directory: {local_temp_dir}")
            shutil.rmtree(local_temp_dir)

if __name__ == "__main__":
    process_gcs_videos_with_blend()