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

    if response.status_code == 200 and response.text:
        try:
            outer = json.loads(response.text)
            inner = outer.get("result")
            if not inner:
                return None
            # If it's a nested JSON string, decode it
            if isinstance(inner, str):
                inner = json.loads(inner)
            # Some versions wrap it inside {"value": {...}}
            return inner.get("value", inner)
        except Exception as e:
            print("Failed to parse job:", str(e))
            return None
    return None

def get_keys(event_id, gallery_type):
    prefix = f"galleries/{event_id}/{gallery_type}/"  # Corrected path
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    keys = []
    for page in pages:
        contents = page.get("Contents", [])
        for obj in contents:
            keys.append(obj["Key"])
    return keys

import datetime

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

        zip_key = f"zips/{event_id}-{gallery_type}.zip"
        s3.upload_file(tmp_zip.name, BUCKET, zip_key)

        # Generate a pre-signed URL (valid for 24 hours)
        zip_url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': BUCKET, 'Key': zip_key},
            ExpiresIn=86400  # 24 hours
        )

        print("ZIP uploaded to:", zip_url)

        payload = {
            "email": email,
            "zip_url": zip_url,
            "event_id": event_id,
            "gallery_type": gallery_type
        }
        print("Sending this payload to Zapier:", json.dumps(payload, indent=2))
        zap_response = requests.post(ZAPIER_WEBHOOK_URL, json=payload)
        print("Zapier response:", zap_response.status_code)
        print("Zapier response body:", zap_response.text)

while True:
    try:
        job = fetch_job()
        if job:
            print("Processing job:", job)
            zip_and_upload(
                event_id=job["event_id"],
                gallery_type=job["gallery_type"],
                email=job["email"]
            )
        else:
            print("No job returned from Redis.")
            time.sleep(2)
    except Exception as e:
        print("Error in main loop:", str(e))
        time.sleep(5)
