
import os
import cv2
import requests
import json
import time
from datetime import timedelta
import subprocess
import shutil
import uuid
import base64
from google import genai
import requests
from google.genai import types
from google.genai.types import GenerateVideosConfig, Image
from google.cloud import storage
from google.auth.transport.requests import Request
from google.auth import default as google_auth_default
from vertexai.generative_models import GenerativeModel, Image
from moviepy.editor import VideoFileClip, concatenate_videoclips, vfx
from PIL import Image, ImageOps
from rembg import remove
import io
from collections import OrderedDict
import glob
import tempfile



# --- Google Cloud Configuration ---
BUCKET_NAME = "veo_exps_prod" 
SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
DESTINATION_PREFIX = "CRED_exps/output_videos/"
FINAL_VIDEO_NAME = "concatenated_fade_video.mp4"
TRANSITION_DURATION = 0.2 # Duration of the fade in/out in seconds.

# --- Vertex AI Configuration ---
PROJECT_ID = "veo-testing" 
LOCATION = "us-central1"  



def process_gcs_videos():
    """
    Downloads videos from a GCS bucket, applies fade transitions,
    concatenates them, and uploads the result back to GCS.
    """
    local_temp_dir = tempfile.mkdtemp()
    print(f"📁 Created temporary directory: {local_temp_dir}")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # 1. Download all videos from the source GCS folder
        # --------------------------------------------------
        print(f"⬇️  Downloading videos from gs://{BUCKET_NAME}/{SOURCE_PREFIX}...")
        blobs = bucket.list_blobs(prefix=SOURCE_PREFIX)
        downloaded_files = []

        for blob in blobs:
            # Skip non-video files or the "folder" itself
            if not blob.name.lower().endswith(('.mp4', '.mov', '.avi')) or blob.name.endswith('/'):
                continue

            file_name = os.path.basename(blob.name)
            local_path = os.path.join(local_temp_dir, file_name)
            blob.download_to_filename(local_path)
            downloaded_files.append(local_path)
            print(f"   - Downloaded {file_name}")

        if not downloaded_files:
            print("⚠️ No video files found in the source directory. Exiting.")
            return

        # Sort files alphabetically to ensure correct order
        downloaded_files.sort()

        # 2. Apply transitions and stitch videos using MoviePy
        # --------------------------------------------------
        print("\n🎬 Applying transitions and stitching videos...")
        processed_clips = []
        num_clips = len(downloaded_files)

        for i, file_path in enumerate(downloaded_files):
            clip = VideoFileClip(file_path)
            
            # Apply fadein to all clips except the first one
            if i > 0:
                clip = clip.fadein(TRANSITION_DURATION)
            
            # Apply fadeout to all clips except the last one
            if i < num_clips - 1:
                clip = clip.fadeout(TRANSITION_DURATION)

            processed_clips.append(clip)
            print(f"   - Processed {os.path.basename(file_path)}")

        # Concatenate all the processed clips
        final_clip = concatenate_videoclips(processed_clips, method="compose")
        final_video_path = os.path.join(local_temp_dir, FINAL_VIDEO_NAME)

        # Write the final video to the temporary directory
        print("\n💾 Writing final concatenated video locally...")
        final_clip.write_videofile(final_video_path, codec="libx264", audio_codec="aac")

        # 3. Upload the final video back to GCS
        # ----------------------------------------
        destination_blob_name = os.path.join(DESTINATION_PREFIX, FINAL_VIDEO_NAME)
        print(f"\n⬆️  Uploading final video to gs://{BUCKET_NAME}/{destination_blob_name}...")
        
        blob_upload = bucket.blob(destination_blob_name)
        blob_upload.upload_from_filename(final_video_path)

        print("✅ Process complete!")

    except Exception as e:
        print(f"❌ An error occurred: {e}")

    finally:
        # 4. Clean up the local temporary directory
        # -----------------------------------------
        if os.path.exists(local_temp_dir):
            print(f"\n🗑️  Cleaning up temporary directory: {local_temp_dir}")
            shutil.rmtree(local_temp_dir)

if __name__ == "__main__":
    process_gcs_videos()