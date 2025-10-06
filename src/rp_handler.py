import runpod
from runpod.serverless.utils import rp_upload
import json
import urllib.request
import urllib.parse
import time
import os
import requests
import base64
from io import BytesIO
####
# ==========================================
# CONFIGURATION
# ==========================================
COMFY_API_AVAILABLE_INTERVAL_MS = 50
COMFY_API_AVAILABLE_MAX_RETRIES = 500
COMFY_POLLING_INTERVAL_MS = int(os.environ.get("COMFY_POLLING_INTERVAL_MS", 250))
COMFY_POLLING_MAX_RETRIES = int(os.environ.get("COMFY_POLLING_MAX_RETRIES", 500))
COMFY_HOST = "127.0.0.1:8188"
REFRESH_WORKER = os.environ.get("REFRESH_WORKER", "false").lower() == "true"


# ==========================================
# VALIDATION
# ==========================================
def validate_input(job_input):
    """Validates the input for the handler function."""
    if job_input is None:
        return None, "Please provide input"

    if isinstance(job_input, str):
        try:
            job_input = json.loads(job_input)
        except json.JSONDecodeError:
            return None, "Invalid JSON format in input"

    workflow = job_input.get("workflow")
    if workflow is None:
        return None, "Missing 'workflow' parameter"

    images = job_input.get("images")
    if images is not None:
        if not isinstance(images, list) or not all(
            any(k in image for k in ("image", "url")) and "name" in image for image in images
        ):
            return (
                None,
                "'images' must be a list of objects with 'name' and either 'image' (base64) or 'url'",
            )

    return {"workflow": workflow, "images": images}, None


# ==========================================
# SERVER CONNECTION
# ==========================================
def check_server(url, retries=500, delay=50):
    """Check if ComfyUI API is reachable."""
    for i in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print("runpod-worker-comfy - API is reachable")
                return True
        except requests.RequestException:
            pass
        time.sleep(delay / 1000)

    print(f"runpod-worker-comfy - Failed to connect to {url}")
    return False


# ==========================================
# IMAGE / VIDEO UPLOAD (Base64 or URL)
# ==========================================
def upload_images(images):
    """Upload files (Base64 or via URL) to ComfyUI /upload/image endpoint."""
    if not images:
        return {"status": "success", "message": "No input files to upload", "details": []}

    responses = []
    upload_errors = []

    print("runpod-worker-comfy - uploading input file(s)...")

    for image in images:
        name = image.get("name")
        blob = None

        try:
            # --- Case 1: URL-based upload ---
            if "url" in image:
                url = image["url"]
                print(f"Downloading input from URL: {url}")
                with requests.get(url, stream=True) as r:
                    if r.status_code == 200:
                        # Stream download to avoid memory overflow
                        temp_path = f"/tmp/{name}"
                        with open(temp_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                        blob = open(temp_path, "rb").read()
                    else:
                        upload_errors.append(f"Failed to download {name}: HTTP {r.status_code}")
                        continue

            # --- Case 2: Base64-based upload ---
            elif "image" in image:
                blob = base64.b64decode(image["image"])

            else:
                upload_errors.append(f"No valid 'url' or 'image' found for {name}")
                continue

            files = {
                "image": (name, BytesIO(blob), "application/octet-stream"),
                "overwrite": (None, "true"),
            }

            response = requests.post(f"http://{COMFY_HOST}/upload/image", files=files)
            if response.status_code != 200:
                upload_errors.append(f"Error uploading {name}: {response.text}")
            else:
                responses.append(f"✅ Uploaded {name}")

        except Exception as e:
            upload_errors.append(f"Error processing {name}: {str(e)}")

    if upload_errors:
        print("runpod-worker-comfy - upload completed with errors")
        return {"status": "error", "message": "Some files failed to upload", "details": upload_errors}

    print("runpod-worker-comfy - all input files uploaded successfully")
    return {"status": "success", "message": "All inputs uploaded successfully", "details": responses}


# ==========================================
# COMFYUI WORKFLOW QUEUE
# ==========================================
def queue_workflow(workflow):
    """Queue a workflow for ComfyUI processing."""
    data = json.dumps({"prompt": workflow}).encode("utf-8")
    req = urllib.request.Request(f"http://{COMFY_HOST}/prompt", data=data)
    return json.loads(urllib.request.urlopen(req).read())


def get_history(prompt_id):
    """Retrieve history of a given ComfyUI prompt."""
    with urllib.request.urlopen(f"http://{COMFY_HOST}/history/{prompt_id}") as response:
        return json.loads(response.read())

##
# ==========================================
# UTILS
# ==========================================
def base64_encode(file_path):
    """Convert file (image/video) to base64 string."""
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


# ==========================================
# PROCESS OUTPUT FILES (MULTI-OUTPUT SUPPORT)
# ==========================================
def process_output_files(outputs, job_id):
    """
    Collects all generated image/video files and returns URLs or base64 strings.
    Supports multiple outputs (images, gifs, videos, etc.)
    """

    COMFY_OUTPUT_PATH = os.environ.get("COMFY_OUTPUT_PATH", "/comfyui/output")
    bucket_url = os.environ.get("BUCKET_ENDPOINT_URL")

    output_files = []

    for node_id, node_output in outputs.items():
        print(f"🔍 Node {node_id} output keys: {list(node_output.keys())}")

        for key in ["images", "gifs", "videos", "output"]:
            if key in node_output:
                for file_obj in node_output[key]:
                    if "filename" not in file_obj:
                        continue
                    file_path = os.path.join(
                        COMFY_OUTPUT_PATH, file_obj.get("subfolder", ""), file_obj["filename"]
                    )
                    output_files.append({
                        "type": "video" if key in ["videos", "output"] else "image",
                        "path": file_path
                    })

    if not output_files:
        print("⚠️ No image/video outputs found")
        return {"status": "error", "message": "No image or video outputs found."}

    results = []

    for item in output_files:
        local_path = item["path"]
        file_type = item["type"]
        filename = os.path.basename(local_path)

        if not os.path.exists(local_path):
            print(f"❌ Missing file: {local_path}")
            results.append({
                "type": file_type,
                "filename": filename,
                "status": "missing",
                "message": f"File not found: {local_path}"
            })
            continue

        # Upload to S3 or encode as base64
        if bucket_url:
            uploaded_url = rp_upload.upload_image(job_id, local_path)
            print(f"✅ Uploaded {filename} to S3")
            results.append({
                "type": file_type,
                "filename": filename,
                "status": "uploaded",
                "url": uploaded_url
            })
        else:
            encoded = base64_encode(local_path)
            print(f"✅ Encoded {filename} to base64")
            results.append({
                "type": file_type,
                "filename": filename,
                "status": "base64",
                "data": encoded
            })

    # Identify primary video (audio version preferred)
    primary_video = None
    for f in results:
        if f["type"] == "video" and "-audio" in f["filename"]:
            primary_video = f
            break
    if not primary_video:
        for f in results:
            if f["type"] == "video":
                primary_video = f
                break

    return {"status": "success", "files": results, "primary_video": primary_video}


# ==========================================
# MAIN HANDLER
# ==========================================
def handler(job):
    """Main RunPod handler that processes a ComfyUI workflow job."""
    job_input = job["input"]

    validated_data, error_message = validate_input(job_input)
    if error_message:
        return {"error": error_message}

    workflow = validated_data["workflow"]
    images = validated_data.get("images")

    # Wait for ComfyUI server availability
    check_server(f"http://{COMFY_HOST}",
                 COMFY_API_AVAILABLE_MAX_RETRIES,
                 COMFY_API_AVAILABLE_INTERVAL_MS)

    # Upload input images/videos (Base64 or via URL)
    upload_result = upload_images(images)
    if upload_result["status"] == "error":
        return upload_result

    # Queue workflow
    try:
        queued = queue_workflow(workflow)
        prompt_id = queued["prompt_id"]
        print(f"runpod-worker-comfy - queued workflow ID: {prompt_id}")
    except Exception as e:
        return {"error": f"Error queuing workflow: {str(e)}"}

    # Poll until workflow completes
    print("runpod-worker-comfy - waiting for generation to complete...")
    retries = 0
    try:
        while retries < COMFY_POLLING_MAX_RETRIES:
            history = get_history(prompt_id)
            if prompt_id in history and history[prompt_id].get("outputs"):
                break
            time.sleep(COMFY_POLLING_INTERVAL_MS / 1000)
            retries += 1
        else:
            return {"error": "Max retries reached waiting for generation"}
    except Exception as e:
        return {"error": f"Error while waiting for generation: {str(e)}"}

    # Process outputs (supports multiple formats)
    outputs = history[prompt_id].get("outputs", {})
    result_files = process_output_files(outputs, job["id"])

    # Include refresh flag for RunPod
    return {**result_files, "refresh_worker": REFRESH_WORKER}


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})
