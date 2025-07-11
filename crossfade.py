import os
import tempfile
import shutil
from moviepy.editor import VideoFileClip, CompositeVideoClip, vfx
from google.cloud import storage

# --- Configuration ---
BUCKET_NAME = "veo_exps_prod" 
SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
DESTINATION_PREFIX = "CRED_exps/output_videos/"
FINAL_VIDEO_NAME = "concatenated_video_crossfade.mp4"
TRANSITION_DURATION = 0.5 # Duration of the blend in seconds

def process_gcs_videos_with_crossfade():
    """
    Downloads videos, applies a true crossfade transition to avoid black screens,
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

        # 2. Apply a true crossfade transition
        print("\n🎬 Applying a robust crossfade transition...")
        clips = [VideoFileClip(f) for f in downloaded_files]
        
        composited_clips_list = []
        current_duration = 0

        for i, clip in enumerate(clips):
            # The first clip is the base layer, no effects needed yet.
            if i == 0:
                composited_clips_list.append(clip.set_start(0))
                current_duration = clip.duration
                continue

            # For all other clips, apply a crossfade-in effect.
            # This makes the clip fade from transparent to opaque.
            start_time = current_duration - TRANSITION_DURATION
            clip_with_fade = (clip.fx(vfx.fadein, TRANSITION_DURATION)
                      .set_start(start_time))
            
            # Add the fading-in clip on top of the previous one.
            # Because the previous clip has no fadeout, it remains visible
            # underneath, preventing any black frames.
            composited_clips_list.append(clip_with_fade)
            current_duration += clip.duration - TRANSITION_DURATION
            print(f"   - Processed {os.path.basename(clip.filename)}")
        
        # Create the final video by layering all the clips
        final_clip = CompositeVideoClip(composited_clips_list).set_duration(current_duration)
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
    process_gcs_videos_with_crossfade()