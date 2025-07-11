import os
import tempfile
import shutil
from moviepy.editor import VideoFileClip, CompositeVideoClip
from google.cloud import storage

# --- Configuration ---
BUCKET_NAME = "veo_exps_prod" 
SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
DESTINATION_PREFIX = "CRED_exps/output_videos/"
FINAL_VIDEO_NAME = "concatenated_video_slide.mp4"
TRANSITION_DURATION = 2 # Duration of the slide in seconds

def process_gcs_videos_with_slide():
    """
    Downloads videos, applies a slide transition between them,
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
        

        # 2. Apply slide transitions using CompositeVideoClip
        print("\n🎬 Applying slide transitions...")
        clips = [VideoFileClip(f) for f in downloaded_files]
        
        video_size = clips[0].size
        w, h = video_size
        
        composited_clips_list = []
        current_duration = 0

        for i, clip in enumerate(clips):
            if i == 0:
                # Add the first clip as the base
                composited_clips_list.append(clip.set_start(0))
                current_duration = clip.duration
            else:
                # For subsequent clips, create the slide effect
                start_time = current_duration - TRANSITION_DURATION

                # This function animates the clip's position
                # It takes 't' (time in seconds within the clip) as input
                def slide_in_position(t):
                    if t < TRANSITION_DURATION:
                        # Animate from right to center
                        return (w - (w / TRANSITION_DURATION) * t, 'center')
                    else:
                        # Hold position at the center
                        return ('center', 'center')

                # Apply the animated position to the clip
                clip_with_slide = clip.set_position(slide_in_position).set_start(start_time)

                composited_clips_list.append(clip_with_slide)
                # Update the total duration, accounting for the overlap
                current_duration += clip.duration - TRANSITION_DURATION
            
            print(f"   - Processed {os.path.basename(clip.filename)}")

        # Create the final video by compositing all clips
        final_clip = CompositeVideoClip(composited_clips_list, size=video_size).set_duration(current_duration)
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
    process_gcs_videos_with_slide()