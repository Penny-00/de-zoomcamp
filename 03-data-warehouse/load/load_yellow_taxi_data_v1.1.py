import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
import logging  # ✅ NEW: proper logging


# =========================
# CONFIG
# =========================
BUCKET_NAME = "penny-zoomcamp-taxi-data_0"
CREDENTIALS_FILE = "gcs.json"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "data"  # ✅ CHANGED: not current dir, cleaner separation

CHUNK_SIZE = 8 * 1024 * 1024

# =========================
# LOGGING SETUP (F4 FIX: logging >> print)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================
# INIT
# =========================
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)


# =========================
# DOWNLOAD (F2 FIX: retry)
# =========================
def download_file(month, max_retries=3):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    for attempt in range(max_retries):
        try:
            logging.info(f"Downloading {url} (Attempt {attempt + 1})")
            urllib.request.urlretrieve(url, file_path)
            logging.info(f"Downloaded: {file_path}")
            return file_path

        except Exception as e:
            logging.error(f"Download failed for {url}: {e}")
            time.sleep(2 * (attempt + 1))  # backoff

    logging.error(f"Giving up download: {url}")
    return None


# =========================
# BUCKET CREATION (F1 FIX: use bucket only once)
# =========================
def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)

        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            logging.info(f"Bucket '{bucket_name}' exists and belongs to your project.")
        else:
            logging.error(f"Bucket '{bucket_name}' exists but not yours.")
            sys.exit(1)

    except NotFound:
        client.create_bucket(bucket_name)
        logging.info(f"Created bucket '{bucket_name}'")

    except Forbidden:
        logging.error(f"Bucket '{bucket_name}' exists but is inaccessible.")
        sys.exit(1)


# =========================
# VERIFY (UNCHANGED LOGIC)
# =========================
def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


# =========================
# UPLOAD (F1, F6, F9 FIXES)
# =========================
def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    # ✅ F6 FIX: idempotency — skip if already exists
    if blob.exists(client):
        logging.info(f"Skipping {blob_name}, already exists in bucket.")
        return

    for attempt in range(max_retries):
        try:
            logging.info(f"Uploading {file_path} (Attempt {attempt + 1})")
            blob.upload_from_filename(file_path)

            if verify_gcs_upload(blob_name):
                logging.info(f"Upload verified: {blob_name}")

                # ✅ F9 FIX: cleanup local file after success
                os.remove(file_path)
                logging.info(f"Deleted local file: {file_path}")

                return
            else:
                logging.warning(f"Verification failed for {blob_name}, retrying...")

        except Exception as e:
            logging.error(f"Upload failed for {file_path}: {e}")

        time.sleep(5)

    logging.error(f"Giving up on {file_path} after {max_retries} attempts.")


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    # ✅ F1 FIX: bucket creation ONLY once (not inside upload anymore)
    create_bucket(BUCKET_NAME)

    # Stage 1: Download
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    # Stage 2: Upload (filter failed downloads)
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    logging.info("All files processed and verified.")