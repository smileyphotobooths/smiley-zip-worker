import os
import redis
import json
import boto3
import tempfile
import zipfile
import requests
from time import sleep

r = redis.Redis.from_url(os.getenv("REDIS_URL"))
s3 = boto3.client("s3")

BUCKET = os.getenv("S3_BUCKET")
ZAPIER_WEBHOOK = os.getenv("ZAPIER_WEBHOOK")

def zip_images(event_id, gallery_type, email):
    prefix = f"galleries/{event_id}/{gallery_type}/"
    result = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    files = result.get("Contents", [])
    
    if not files:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, f"{event_id}-{gallery_type}.zip")
        with zipfile.ZipFile(zip_path, "w") as zipf:
            for obj in files:
                key = obj["Key"]
                filename = key.split("/")[-1]
                temp_file = os.path.join(tmpdir, filename)
                s3.download_file(BUCKET, key, temp_file)
                zipf.write(temp_file, arcname=filename)

        upload_key = f"zips/{event_id}-{gallery_type}.zip"
        s3.upload_file(zip_path, BUCKET, upload_key)

        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": BUCKET, "Key": upload_key},
            ExpiresIn=604800  # 7 days
        )

while True:
    job_data = r.lpop("zip_jobs")
    if not job_data:
        sleep(2)
        continue

    job = json.loads(job_data)
    event_id = job.get("event_id")
    gallery_type = job.get("type")
    email = job.get("email")

    url = zip_images(event_id, gallery_type, email)

    if url:
        payload = {"email": email, "url": url}
        requests.post(ZAPIER_WEBHOOK, json=payload)
