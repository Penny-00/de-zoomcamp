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
# CREDENTIALS_FILE = "gcs.json"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "data"  # ✅ CHANGED: not current dir, cleaner separation
UPLOAD_ONLY = "--upload-only" in sys.argv

CHUNK_SIZE = 8 * 1024 * 1024
DOWNLOAD_WORKERS = 4
UPLOAD_WORKERS = 2
UPLOAD_TIMEOUT_SECONDS = 600
UPLOAD_RETRY_DELAY_SECONDS = 10

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

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


# =========================
# DOWNLOAD (F2 FIX: retry)
# =========================
def download_file(month, max_retries=3):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    if os.path.exists(file_path):
        logging.info(f"Skipping download, already exists: {file_path}")
        return file_path

    for attempt in range(max_retries):
        try:
            logging.info(f"Downloading {url} (Attempt {attempt + 1})")
            urllib.request.urlretrieve(url, file_path)
            logging.info(f"Downloaded: {file_path}")
            return file_path

        except Exception as e:
            logging.error(f"Download failed for {url}: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
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
        return True

    for attempt in range(max_retries):
        try:
            logging.info(f"Uploading {file_path} (Attempt {attempt + 1})")
            blob.upload_from_filename(file_path, timeout=UPLOAD_TIMEOUT_SECONDS)

            if verify_gcs_upload(blob_name):
                logging.info(f"Upload verified: {blob_name}")

                # ✅ F9 FIX: cleanup local file after success
                os.remove(file_path)
                logging.info(f"Deleted local file: {file_path}")

                return True
            else:
                logging.warning(f"Verification failed for {blob_name}, retrying...")

        except Exception as e:
            logging.error(f"Upload failed for {file_path}: {e}")

        time.sleep(UPLOAD_RETRY_DELAY_SECONDS)

    logging.error(f"Giving up on {file_path} after {max_retries} attempts.")
    return False


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    # ✅ F1 FIX: bucket creation ONLY once (not inside upload anymore)
    create_bucket(BUCKET_NAME)

    downloaded_files = []
    failed_downloads = []

    if not UPLOAD_ONLY:
        # Stage 1: Download
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
            file_paths = list(executor.map(download_file, MONTHS))
        downloaded_files = [path for path in file_paths if path is not None]
        failed_downloads = [month for month, path in zip(MONTHS, file_paths) if path is None]
    else:
        downloaded_files = [
            os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
            for month in MONTHS
            if os.path.exists(os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet"))
        ]
        missing_files = [
            month for month in MONTHS
            if not os.path.exists(os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet"))
        ]
        if missing_files:
            logging.error(f"Missing local files for upload-only mode: {', '.join(missing_files)}")
            sys.exit(1)

    # Stage 2: Upload (filter failed downloads)
    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as executor:
        upload_results = list(executor.map(upload_to_gcs, downloaded_files))

    failed_uploads = [
        path for path, success in zip(downloaded_files, upload_results) if not success
    ]

    if failed_downloads or failed_uploads:
        if failed_downloads:
            logging.error(f"Failed downloads (months): {', '.join(failed_downloads)}")
        if failed_uploads:
            logging.error(f"Failed uploads (files): {', '.join(failed_uploads)}")
        logging.error("Completed with failures.")
        sys.exit(1)

    logging.info("All files downloaded and uploaded successfully.")