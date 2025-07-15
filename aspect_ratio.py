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
from moviepy.editor import VideoFileClip, concatenate_videoclips, vfx, CompositeVideoClip, ColorClip
from PIL import Image, ImageOps
from rembg import remove
import io
from collections import OrderedDict
import glob
import tempfile
import math



# --- Google Cloud Configuration ---
BUCKET_NAME = "veo_exps_prod" 
SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
DESTINATION_PREFIX = "CRED_exps/output_videos/altered_aspect_ratio/"

# --- Vertex AI Configuration ---
PROJECT_ID = "veo-testing" 
LOCATION = "us-central1"  



def process_aspect_ratio_in_gcs():
    """
    Downloads videos, swaps aspect ratio between 16:9 and 9:16,
    and uploads them back to GCS.
    """
    local_temp_dir = tempfile.mkdtemp()
    print(f"📁 Created temporary directory: {local_temp_dir}")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        blobs_to_process = list(bucket.list_blobs(prefix=SOURCE_PREFIX))
        print(f"Found {len(blobs_to_process)} files to check in source folder.")

        for blob in blobs_to_process:
            if blob.name.endswith('/'):
                continue

            video_filename = os.path.basename(blob.name)
            local_input_path = os.path.join(local_temp_dir, video_filename)

            print(f"\n⬇️  Downloading {video_filename}...")
            blob.download_to_filename(local_input_path)

            # The 'with' block ensures the clip is properly closed after all operations
            with VideoFileClip(local_input_path) as clip:
                processed_clip = None
                w, h = clip.size
                if h == 0:
                    print(f"SKIPPING: Invalid video dimensions for {video_filename}.")
                    continue
                
                ar = w / h
                
                # Check for 16:9 (Landscape)
                if math.isclose(ar, 16/9, rel_tol=0.02):
                    print(f"Processing 16:9 video -> 9:16 (Center Crop)...")
                    crop_width = h * (9 / 16)
                    x1 = (w / 2) - (crop_width / 2)
                    processed_clip = clip.crop(x1=x1, width=crop_width)
                
                # Check for 9:16 (Portrait)
                elif math.isclose(ar, 9/16, rel_tol=0.02):
                    print(f"Processing 9:16 video -> 16:9 (Pillarbox)...")
                    target_size = (h, w)
                    background = ColorClip(
                        size=target_size, color=(0, 0, 0), duration=clip.duration
                    )
                    processed_clip = CompositeVideoClip([
                        background, clip.set_position("center")
                    ])
                else:
                    print(f"SKIPPING: {video_filename} is not 16:9 or 9:16 (AR: {ar:.2f}).")

                # --- THIS ENTIRE BLOCK IS NOW INDENTED TO BE INSIDE 'with' ---
                if processed_clip:
                    local_output_path = os.path.join(local_temp_dir, f"processed_{video_filename}")
                    
                    print(f"Writing processed video locally...")
                    processed_clip.write_videofile(
                        local_output_path, codec="libx264", audio_codec="aac"
                    )

                    destination_blob_name = os.path.join(DESTINATION_PREFIX, video_filename)
                    print(f"⬆️  Uploading to gs://{BUCKET_NAME}/{destination_blob_name}...")
                    
                    upload_blob = bucket.blob(destination_blob_name)
                    upload_blob.upload_from_filename(local_output_path)
                    
                    print(f"✅ Successfully processed and uploaded {video_filename}.")
                    # No need to manually close processed_clip, the parent 'with' handles resources.

    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        print(f"\n🗑️  Cleaning up temporary directory...")
        shutil.rmtree(local_temp_dir)

if __name__ == "__main__":
    process_aspect_ratio_in_gcs()