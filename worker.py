import os
import time
import json
import tempfile
import zipfile
import boto3
import requests

# === Redis Setup ===
UPSTASH_REDIS_REST_URL = os.environ["UPSTASH_REDIS_REST_URL"]
UPSTASH_REDIS_REST_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

# === AWS S3 Setup ===
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "us-west-1")
BUCKET = os.environ["S3_BUCKET_NAME"]

# === Zapier Webhook ===
ZAPIER_WEBHOOK_URL = os.environ["ZAPIER_WEBHOOK_URL"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def fetch_job():
    response = requests.post(
        f"{UPSTASH_REDIS_REST_URL}/lpop/zip_jobs",
        headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
    )
    print("Raw Redis response:", response.status_code, response.text)

    if response.status_code == 200:
        try:
            data = json.loads(response.text)

            if not data or not data.get("result"):
                print("No job returned from Redis.")
                return None

            raw = data["result"]

            # If result is a stringified JSON object, decode it again
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError as e:
                    print("Failed to decode job string:", raw)
                    return None

            # If wrapped inside a 'value' key (like Zapier sends), unwrap it
            if isinstance(raw, dict) and "value" in raw:
                raw = raw["value"]

            # Final check: must have required fields
            if all(k in raw for k in ("event_id", "gallery_type", "email")):
                return raw
            else:
                print("Job missing required fields:", raw)
        except Exception as e:
            print("Error decoding Redis job:", str(e))

    return None

def get_keys(event_id, gallery_type):
    prefix = f"galleries/{event_id}/{gallery_type}/"  # <- ADD 'galleries/' prefix
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    keys = []
    for page in pages:
        contents = page.get("Contents", [])
        for obj in contents:
            keys.append(obj["Key"])
    return keys

def zip_and_upload(event_id, gallery_type, email):
    keys = get_keys(event_id, gallery_type)
    if not keys:
        print(f"No files found for {event_id}/{gallery_type}")
        return

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
        with zipfile.ZipFile(tmp_zip.name, 'w') as zipf:
            for key in keys:
                with tempfile.NamedTemporaryFile() as tmp_file:
                    s3.download_file(BUCKET, key, tmp_file.name)
                    zipf.write(tmp_file.name, arcname=os.path.basename(key))

        zip_key = f"{event_id}/zips/{gallery_type}.zip"
        s3.upload_file(tmp_zip.name, BUCKET, zip_key)
        zip_url = f"https://{BUCKET}.s3.{AWS_REGION}.amazonaws.com/{zip_key}"
        print("ZIP uploaded to:", zip_url)

        payload = {
            "email": email,
            "zip_url": zip_url,
            "event_id": event_id,
            "gallery_type": gallery_type
        }
        zap_response = requests.post(ZAPIER_WEBHOOK_URL, json=payload)
        print("Zapier response:", zap_response.status_code)

while True:
    try:
        job = fetch_job()
        if job:
            print("Processing job:", job)

            # Handle Zapier's nested format if needed
            if isinstance(job, dict) and 'value' in job and isinstance(job['value'], dict):
                job = job['value']

            zip_and_upload(
                event_id=job["event_id"],
                gallery_type=job["gallery_type"],
                email=job["email"]
            )
        else:
            time.sleep(2)
    except Exception as e:
        print("Error in main loop:", str(e))
        time.sleep(5)
