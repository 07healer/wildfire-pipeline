from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/g100004569/gcp-credentials.json"

def upload_to_gcs(local_file, bucket_name, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    
    # increase chunk size + timeout
    blob.chunk_size = 5 * 1024 * 1024  # 5MB chunks
    blob.upload_from_filename(local_file, timeout=600)
    
    print(f"Uploaded {local_file} to gs://{bucket_name}/{destination_blob}")

BUCKET = "fire-pipeline-bucket"

upload_to_gcs("historical_fires.parquet", BUCKET, "raw/historical_fires.parquet")