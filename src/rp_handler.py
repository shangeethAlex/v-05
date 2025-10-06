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

    inputs = job_input.get("inputs")
    if inputs is not None:
        if not isinstance(inputs, list) or not all(
            any(k in inp for k in ("image", "url")) and "name" in inp for inp in inputs
        ):
            return (
                None,
                "'inputs' must be a list of objects with 'name' and either 'image' (base64) or 'url'",
            )

    return {"workflow": workflow, "inputs": inputs}, None

# ==========================================
# SERVER CONNECTION
# ==========================================
def check_server(url, retries=500, delay=50):
    """Check if ComfyUI API is reachable."""
    for _ in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print("✅ ComfyUI API is reachable")
                return True
        except requests.RequestException:
            pass
        time.sleep(delay / 1000)
    print(f"❌ Failed to connect to {url}")
    return False

# ==========================================
# FILE UPLOAD (Base64 or URL)
# ==========================================
def upload_inputs(inputs):
    """Upload files (Base64 or via URL) to ComfyUI.
    - Images → uploaded via /upload/image
    - Videos → saved directly to /comfyui/input/
    """
    if not inputs:
        return {"status": "success", "message": "No input files to upload", "details": []}

    responses = []
    upload_errors = []

    print("🚀 Uploading input file(s)...")

    for inp in inputs:
        name = inp.get("name")
        blob = None

        try:
            # --- Case 1: URL-based upload ---
            if "url" in inp:
                url = inp["url"]
                print(f"📥 Downloading from URL: {url}")
                with requests.get(url, stream=True) as r:
                    if r.status_code == 200:
                        temp_path = f"/tmp/{name}"
                        with open(temp_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                        blob = open(temp_path, "rb").read()
                    else:
                        upload_errors.append(f"Failed to download {name}: HTTP {r.status_code}")
                        continue

            # --- Case 2: Base64-based upload ---
            elif "image" in inp:
                blob = base64.b64decode(inp["image"])

            else:
                upload_errors.append(f"No valid 'url' or 'image' found for {name}")
                continue

            # ==========================================
            # Decide based on file type
            # ==========================================
            if name.lower().endswith((".mp4", ".mov", ".avi", ".mkv")):
                # 🔸 Directly save video file to input folder
                dest_path = f"/comfyui/input/{name}"
                with open(dest_path, "wb") as f:
                    f.write(blob)
                responses.append(f"✅ Saved video directly to {dest_path}")
                print(f"✅ Video file saved: {dest_path}")

            else:
                # 🔹 Use /upload/image for images
                files = {
                    "image": (name, BytesIO(blob), "application/octet-stream"),
                    "overwrite": (None, "true"),
                }
                response = requests.post(f"http://{COMFY_HOST}/upload/image", files=files)

                if response.status_code != 200:
                    upload_errors.append(f"Error uploading {name}: {response.text}")
                else:
                    responses.append(f"✅ Uploaded {name} via /upload/image")
                    print(f"✅ Image uploaded: {name}")

        except Exception as e:
            upload_errors.append(f"Error processing {name}: {str(e)}")

    # ==========================================
    # Summarize upload results
    # ==========================================
    if upload_errors:
        print("⚠️ Upload completed with errors")
        return {
            "status": "error",
            "message": "Some files failed to upload or save",
            "details": upload_errors,
            "success": responses,
        }

    print("✅ All input files uploaded successfully")
    return {
        "status": "success",
        "message": "All inputs uploaded successfully",
        "details": responses,
    }

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

# ==========================================
# UTILS
# ==========================================
def base64_encode(file_path):
    """Convert file (image/video) to base64 string."""
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

# ==========================================
# PROCESS OUTPUT FILES
# ==========================================
def process_output_files(outputs, job_id):
    """Collects generated image/video files and returns URLs or base64 strings."""
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
                    file_path = os.path.join(COMFY_OUTPUT_PATH, file_obj.get("subfolder", ""), file_obj["filename"])
                    output_files.append({
                        "type": "video" if key in ["videos", "output"] else "image",
                        "path": file_path
                    })

    if not output_files:
        print("⚠️ No image/video outputs found")
        return {"status": "error", "message": "No outputs found"}

    results = []
    for item in output_files:
        local_path = item["path"]
        file_type = item["type"]
        filename = os.path.basename(local_path)

        if not os.path.exists(local_path):
            results.append({
                "type": file_type,
                "filename": filename,
                "status": "missing",
                "message": f"File not found: {local_path}"
            })
            continue

        if bucket_url:
            uploaded_url = rp_upload.upload_image(job_id, local_path)
            results.append({
                "type": file_type,
                "filename": filename,
                "status": "uploaded",
                "url": uploaded_url
            })
        else:
            encoded = base64_encode(local_path)
            results.append({
                "type": file_type,
                "filename": filename,
                "status": "base64",
                "data": encoded
            })

    primary_video = next((f for f in results if f["type"] == "video" and "-audio" in f["filename"]), None)
    if not primary_video:
        primary_video = next((f for f in results if f["type"] == "video"), None)

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
    inputs = validated_data.get("inputs")

    check_server(f"http://{COMFY_HOST}", COMFY_API_AVAILABLE_MAX_RETRIES, COMFY_API_AVAILABLE_INTERVAL_MS)

    upload_result = upload_inputs(inputs)
    if upload_result["status"] == "error":
        return upload_result

    try:
        queued = queue_workflow(workflow)
        prompt_id = queued["prompt_id"]
        print(f"🧠 Queued workflow ID: {prompt_id}")
    except Exception as e:
        return {"error": f"Error queuing workflow: {str(e)}"}

    print("⏳ Waiting for workflow completion...")
    retries = 0
    try:
        while retries < COMFY_POLLING_MAX_RETRIES:
            history = get_history(prompt_id)
            if prompt_id in history and history[prompt_id].get("outputs"):
                break
            time.sleep(COMFY_POLLING_INTERVAL_MS / 1000)
            retries += 1
        else:
            return {"error": "Max retries reached while waiting for generation"}
    except Exception as e:
        return {"error": f"Error polling workflow: {str(e)}"}

    outputs = history[prompt_id].get("outputs", {})
    result_files = process_output_files(outputs, job["id"])

    return {**result_files, "refresh_worker": REFRESH_WORKER}

# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})
