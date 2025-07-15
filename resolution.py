
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
DESTINATION_PREFIX = "CRED_exps/output_videos/1080p/"


# --- Vertex AI Configuration ---
PROJECT_ID = "veo-testing" 
LOCATION = "us-central1"  


def resize_videos_in_gcs():
    """
    Downloads videos from a GCS folder, resizes them to 1080p, 
    and uploads them to another folder.
    """
    # Create a temporary local directory to work in
    local_temp_dir = tempfile.mkdtemp()
    print(f"📁 Created temporary directory: {local_temp_dir}")

    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # List all video files in the source folder
        blobs_to_process = [
            blob for blob in bucket.list_blobs(prefix=SOURCE_PREFIX) 
            if not blob.name.endswith('/')
        ]

        if not blobs_to_process:
            print(f"⚠️ No video files found in gs://{BUCKET_NAME}/{SOURCE_PREFIX}")
            return

        print(f"Found {len(blobs_to_process)} videos to process.")

        # Process each video one by one
        for blob in blobs_to_process:
            video_filename = os.path.basename(blob.name)
            local_input_path = os.path.join(local_temp_dir, video_filename)
            local_output_path = os.path.join(local_temp_dir, f"resized_{video_filename}")

            # 1. Download the video from GCS
            print(f"\n⬇️  Downloading {video_filename}...")
            blob.download_to_filename(local_input_path)

            # 2. Alter the resolution using MoviePy
            print(f"Resizing {video_filename} to 1080p...")
            with VideoFileClip(local_input_path) as clip:
                # The core resizing logic: upscale so the height is 1080 pixels
                resized_clip = clip.fx(vfx.resize, height=1080)
                
                # Write the new resized video file locally
                resized_clip.write_videofile(
                    local_output_path, 
                    codec="libx264", 
                    audio_codec="aac"
                )
            
            # 3. Upload the resized video back to GCS
            destination_blob_name = os.path.join(DESTINATION_PREFIX, video_filename)
            print(f"⬆️  Uploading resized video to gs://{BUCKET_NAME}/{destination_blob_name}...")
            
            upload_blob = bucket.blob(destination_blob_name)
            upload_blob.upload_from_filename(local_output_path)
            
            print(f"✅ Successfully processed and uploaded {video_filename}.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")

    finally:
        # Clean up the local temporary directory and all its contents
        print(f"\n🗑️  Cleaning up temporary directory...")
        shutil.rmtree(local_temp_dir)

if __name__ == "__main__":
    resize_videos_in_gcs()