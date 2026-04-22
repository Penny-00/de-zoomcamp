import urllib.request
import urllib.error
import socket
import time
from pathlib import Path
from google.cloud import storage

# Config
BUCKET_NAME = "penny-zoomcamp-taxi-data_0"
FILE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
LOCAL_FILE = "yellow_tripdata_2024-01.parquet"
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 2
DOWNLOAD_TIMEOUT_SECONDS = 60

# Init client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def retry(operation_name, fn):
    """Retry transient network operations with exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except (urllib.error.URLError, socket.gaierror, TimeoutError) as err:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"{operation_name} failed after {MAX_RETRIES} attempts"
                ) from err

            delay = BASE_DELAY_SECONDS ** attempt
            print(
                f"{operation_name} failed on attempt {attempt}/{MAX_RETRIES}: {err}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)


def download_file():
    if Path(LOCAL_FILE).exists():
        print(f"Local file already exists, skipping download: {LOCAL_FILE}")
        return

    print("Downloading file...")
    urllib.request.urlretrieve(FILE_URL, LOCAL_FILE)
    print("Download complete.")


def upload_to_gcs():
    print("Uploading to GCS...")
    blob = bucket.blob(f"raw/{LOCAL_FILE}")
    blob.upload_from_filename(LOCAL_FILE, timeout=DOWNLOAD_TIMEOUT_SECONDS)
    print("Upload complete.")

retry("Download", download_file)
retry("Upload", upload_to_gcs)
print("Done.")