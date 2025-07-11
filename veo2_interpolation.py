import os
import tempfile
import shutil
import json
import time
import base64
import requests
import cv2

from moviepy.editor import VideoFileClip, concatenate_videoclips, vfx
from google.cloud import storage
from google.auth.transport.requests import Request
from google.auth import default as google_auth_default
from google.cloud import storage

# --- Configuration ---
PROJECT_ID = "veo-testing" 
LOCATION = "us-central1"  
BUCKET_NAME = "veo_exps_prod" 
SOURCE_PREFIX = "CRED_exps/input_videos"  # Path to the folder of input videos you want to download
DESTINATION_PREFIX = "CRED_exps/output_videos/"
FINAL_VIDEO_NAME = "final_video_with_interpolated_transitions.mp4"
TRANSITION_DURATION = 1.0  # Duration of the transition in seconds
TRANSITION_SPEED = 5.0     # e.g., 2.0 means the transition plays at 2x speed
INTERPOLATION_PROMPT = "Smoothly transition from the first slate to the last slate without making the characters move too much"


def get_auth_headers() -> dict:
    """
    Authenticates with Google Cloud and returns authorization headers.
    """
    credentials, project_id = google_auth_default()
    credentials.refresh(Request()) # Ensure credentials are fresh
    return {"Authorization": f"Bearer {credentials.token}", "Content-Type": "application/json"}


# --- Placeholder for AI Frame Interpolation ---
def interpolate_video_veo2(
    start_image_path: str,
    end_image_path: str,
    prompt_text: str,
    output_local_video_path: str,
) -> str | None:
    """
    Calls the Veo2 API to generate a video using interpolation from two frames.
    This function handles the long-running operation and returns the final API response.
    """
    print(f"Performing Veo 2 Interpolation: from '{os.path.basename(start_image_path)}' "
          f"to '{os.path.basename(end_image_path)}'")
    
    generated_videos_uri = None

    try:
        with open(start_image_path, "rb") as f:
            start_frame_bytes = f.read()
        start_frame_base64 = base64.b64encode(start_frame_bytes).decode('utf-8')

        with open(end_image_path, "rb") as f:
            end_frame_bytes = f.read()
        end_frame_base64 = base64.b64encode(end_frame_bytes).decode('utf-8')
    
    except Exception as e:
        print(f"  ERROR: Failed to read and encode local image files to base64: {e}")
        return None
    
    target_output_video_gcs_uri = f"gs://{BUCKET_NAME}/{output_local_video_path}"

    api_url = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/us-central1/publishers/google/models/veo-2.0-generate-exp:predictLongRunning" 
    new_url = f"https://us-central1-aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/us-central1/publishers/google/models/veo-2.0-generate-exp:fetchPredictOperation"
    headers = get_auth_headers()

    # Determine MIME type for start_image
    start_image_ext = os.path.splitext(start_image_path)[1].lower()
    if start_image_ext == ".png":
        start_mime_type = "image/png"
    elif start_image_ext in [".jpg", ".jpeg"]:
        start_mime_type = "image/jpeg"
    else:
        print(f"  ERROR: Unsupported file extension for start image: {start_image_ext}")
        return None
    
    # Determine MIME type for end_image
    end_image_ext = os.path.splitext(end_image_path)[1].lower()
    if end_image_ext == ".png":
        end_mime_type = "image/png"
    elif end_image_ext in [".jpg", ".jpeg"]:
        end_mime_type = "image/jpeg"
    else:
        print(f"  ERROR: Unsupported file extension for end image: {end_image_ext}")
        return None
    
    request_body = {
        "instances": [{
            "prompt": prompt_text,
            "image": {"bytesBase64Encoded": start_frame_base64, "mimeType": start_mime_type}, # Using 'content' for base64
            "lastFrame": {"bytesBase64Encoded": end_frame_base64, "mimeType": end_mime_type} # Using 'content'
        }],
        "parameters": {
            "aspectRatio": "16:9",
            "durationSeconds": 5,
            "sampleCount" : 1,
            "storageUri": target_output_video_gcs_uri,
        }
    }
    
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(request_body))
        response.raise_for_status() 
        
        operation_details = response.json()
        op_name = operation_details.get('name', 'N/A')

        # print(f"API Response: {operation_details}")
        print(f"  SUCCESS (LRO Initiated): Veo 2 API call successful. Operation: {op_name}")

        max_iterations = 600
        interval_sec = 10

        new_request_body = {
            "operationName": op_name,
        }

        for i in range(max_iterations):
            try:
                polling_response = requests.post(new_url, headers=headers, data=json.dumps(new_request_body))
                polling_response.raise_for_status()
                polling_data = polling_response.json()

                if polling_data.get("done"):
                    print(f"  SUCCESS (LRO Complete): Operation {op_name} finished.")
                    generated_videos_uri = (
                        polling_data["response"]["videos"][0].get("gcsUri")
                    )
                    print(f"  Generated video available at: {generated_videos_uri}")
                    return generated_videos_uri
                else:
                    print(f"  Polling operation {op_name}, iteration {i+1}. Not done yet. Retrying in {interval_sec} seconds...")

            except requests.exceptions.RequestException as e:
                print(f"  ERROR: Polling failed for operation {op_name}: {e}")
                break  # Exit polling loop on error.
            except KeyError as e:
                print(f"  ERROR: KeyError during polling for {op_name}: {e}. Response: {polling_response.text}")
                break
            except Exception as e:
                print(f"  ERROR: An unexpected error occurred during polling: {e}")
                break
            
            time.sleep(interval_sec) # Wait before the next poll
        
        print(f"  ERROR: Polling timed out for operation {op_name} after {max_iterations} attempts.")

    except requests.exceptions.HTTPError as e:
        print(f"  ERROR: HTTP Error during Veo 2 API call (with bytes): {e.response.status_code} - {e.response.text}")
        print(f"           This may indicate the API does not support byte content for images, expecting gcsUri.")
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: Network or other Request Error during Veo 2 API call (with bytes): {e}")
    except Exception as e:
        print(f"  ERROR: An unexpected error occurred during Veo 2 API call (with bytes): {e}")
    
    return generated_videos_uri

# --- Main GCS Processing Function ---
def process_gcs_videos():
    """Main function to download, process, and upload videos."""
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

        # 2. Process clips and create transitions
        print("\n🎬 Processing videos and generating transitions...")
        clips = [VideoFileClip(p) for p in downloaded_files]
        final_sequence = []

        for i in range(len(clips) - 1):
            clip_A = clips[i]
            clip_B = clips[i+1]
            print(f"\n   - Processing transition between '{os.path.basename(clip_A.filename)}' and '{os.path.basename(clip_B.filename)}'")

            # Add the main body of the first clip
            final_sequence.append(clip_A)
            
            video_capture_A = cv2.VideoCapture(clip_A.filename)
            video_capture_B = cv2.VideoCapture(clip_B.filename)

            # --- Extract last frame of clip A ---
            total_frames_clip_A = int(video_capture_A.get(cv2.CAP_PROP_FRAME_COUNT))
            video_capture_A.set(cv2.CAP_PROP_POS_FRAMES, total_frames_clip_A - 1)
            success_A, last_frame_A = video_capture_A.read()
            frame_A_path = os.path.join(local_temp_dir, f"frame_A_end_{i}.png")
            if success_A: cv2.imwrite(frame_A_path, last_frame_A)

            # --- Extract first frame of clip B ---
            success_B, first_frame_B = video_capture_B.read()
            frame_B_path = os.path.join(local_temp_dir, f"frame_B_start_{i}.png")
            if success_B: cv2.imwrite(frame_B_path, first_frame_B)

            video_capture_A.release()
            video_capture_B.release()

            if not (success_A and success_B):
                print("  WARNING: Failed to extract frames. Skipping transition.")
                continue

            # Generate the transition clip via Veo 2 API
            transition_output_gcs_path = f"{DESTINATION_PREFIX}transitions/transition_{i}"
            transition_gcs_uri = interpolate_video_veo2(
                frame_A_path, frame_B_path, INTERPOLATION_PROMPT, transition_output_gcs_path
            )

            if not transition_gcs_uri:
                print("  WARNING: Veo 2 interpolation failed. Skipping this transition.")
                continue

            # Download the generated transition video from GCS
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            transition_blob_name = transition_gcs_uri.replace(f"gs://{BUCKET_NAME}/", "")
            local_transition_path = os.path.join(local_temp_dir, f"transition_{i}.mp4")
            
            print(f"   - Downloading generated transition from {transition_gcs_uri}")
            bucket.blob(transition_blob_name).download_to_filename(local_transition_path)

            if not os.path.exists(local_transition_path):
                print("  WARNING: Failed to download transition video. Skipping.")
                continue

            transition_clip = VideoFileClip(local_transition_path)

            # Alter the speed of the transition
            sped_up_transition = transition_clip.fx(vfx.speedx, TRANSITION_SPEED)
            final_sequence.append(sped_up_transition)
        
        # Add the final clip in the sequence
        final_sequence.append(clips[-1])
        
        # 3. Concatenate all parts and write the final video
        print("\n🧩 Concatenating final video...")
        final_clip = concatenate_videoclips(final_sequence)
        final_video_path = os.path.join(local_temp_dir, FINAL_VIDEO_NAME)
        
        print(f"💾 Writing final video locally to {final_video_path}...")
        final_clip.write_videofile(final_video_path, codec="libx264", audio_codec="aac")

        # 4. Upload final video to GCS
        destination_blob_name = os.path.join(DESTINATION_PREFIX, FINAL_VIDEO_NAME)
        print(f"\n⬆️  Uploading final video to gs://{BUCKET_NAME}/{destination_blob_name}...")
        
        blob_upload = bucket.blob(destination_blob_name)
        blob_upload.upload_from_filename(final_video_path)
        print("✅ Process complete!")


    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        print(f"\n🗑️  Cleaning up temporary directory...")
        shutil.rmtree(local_temp_dir)

if __name__ == "__main__":
    process_gcs_videos()