import argparse
import logging
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import storage


# =========================
# DEFAULT CONFIG
# =========================
DEFAULT_BUCKET_NAME = "penny-zoomcamp-taxi-data_0"
DEFAULT_DOWNLOAD_DIR = "data"
BASE_URL_TEMPLATE = (
    "https://ghfast.top/https://github.com/DataTalksClub/nyc-tlc-data/"
    "releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz"
)

# Default targets requested by user:
# - yellow taxi: each month in 2020
# - green taxi: each month in 2019 and 2020
DEFAULT_TARGETS = {
    "yellow": [2020],
    "green": [2019, 2020],
}
DEFAULT_MONTHS = [f"{i:02d}" for i in range(1, 13)]

CHUNK_SIZE = 8 * 1024 * 1024
DOWNLOAD_WORKERS = 4
UPLOAD_WORKERS = 2
DOWNLOAD_RETRIES = 3
UPLOAD_RETRIES = 3
UPLOAD_TIMEOUT_SECONDS = 600
UPLOAD_RETRY_DELAY_SECONDS = 10


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download monthly yellow/green taxi CSV files and upload to GCS. "
            "Defaults: yellow-2020, green-2019/2020."
        )
    )
    parser.add_argument(
        "--bucket-name",
        default=DEFAULT_BUCKET_NAME,
        help="GCS bucket name.",
    )
    parser.add_argument(
        "--download-dir",
        default=DEFAULT_DOWNLOAD_DIR,
        help="Local directory for downloaded files.",
    )
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help="Skip downloads and upload only files that already exist locally.",
    )
    parser.add_argument(
        "--taxi-types",
        nargs="+",
        choices=["yellow", "green"],
        default=["yellow", "green"],
        help="Taxi datasets to process.",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        help=(
            "Override years for all selected taxi types. "
            "Example: --years 2020"
        ),
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=DEFAULT_MONTHS,
        help=(
            "Months to process in MM format. "
            "Example: --months 01 02 03"
        ),
    )
    return parser.parse_args()


def build_targets(args: argparse.Namespace) -> dict:
    targets = {}
    for taxi_type in args.taxi_types:
        if args.years:
            years = args.years
        else:
            years = DEFAULT_TARGETS[taxi_type]
        targets[taxi_type] = sorted(set(years))
    return targets


def validate_months(months):
    valid_months = set(DEFAULT_MONTHS)
    for month in months:
        if month not in valid_months:
            raise ValueError(
                f"Invalid month '{month}'. Use MM format between 01 and 12."
            )


def create_gcs_client_and_bucket(bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    try:
        client.get_bucket(bucket_name)
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            logging.info("Bucket '%s' exists and belongs to your project.", bucket_name)
        else:
            logging.error("Bucket '%s' exists but does not belong to your project.", bucket_name)
            sys.exit(1)
    except NotFound:
        client.create_bucket(bucket_name)
        logging.info("Created bucket '%s'.", bucket_name)
    except Forbidden:
        logging.error("Bucket '%s' exists but is inaccessible.", bucket_name)
        sys.exit(1)

    return client, bucket


def build_jobs(targets: dict, months, download_dir: str):
    jobs = []
    for taxi_type, years in targets.items():
        for year in years:
            for month in months:
                file_name = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
                url = BASE_URL_TEMPLATE.format(
                    taxi_type=taxi_type,
                    year=year,
                    month=month,
                )
                file_path = os.path.join(download_dir, file_name)
                blob_name = f"{taxi_type}/{year}/{file_name}"
                jobs.append(
                    {
                        "taxi_type": taxi_type,
                        "year": year,
                        "month": month,
                        "url": url,
                        "file_name": file_name,
                        "file_path": file_path,
                        "blob_name": blob_name,
                    }
                )
    return jobs


def download_job(job: dict, max_retries: int = DOWNLOAD_RETRIES):
    file_path = job["file_path"]
    url = job["url"]

    if os.path.exists(file_path):
        logging.info("Skipping download, file exists: %s", file_path)
        return True

    for attempt in range(max_retries):
        try:
            logging.info(
                "Downloading %s (attempt %d/%d)",
                url,
                attempt + 1,
                max_retries,
            )
            urllib.request.urlretrieve(url, file_path)
            logging.info("Downloaded: %s", file_path)
            return True
        except Exception as exc:
            logging.error("Download failed for %s: %s", url, exc)
            if os.path.exists(file_path):
                os.remove(file_path)
            time.sleep(2 * (attempt + 1))

    logging.error("Giving up download: %s", url)
    return False


def verify_gcs_upload(client: storage.Client, bucket, blob_name: str) -> bool:
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_job(job: dict, client: storage.Client, bucket, max_retries: int = UPLOAD_RETRIES):
    file_path = job["file_path"]
    blob_name = job["blob_name"]
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    if blob.exists(client):
        logging.info("Skipping upload, blob exists: %s", blob_name)
        return True

    for attempt in range(max_retries):
        try:
            logging.info(
                "Uploading %s to gs://%s/%s (attempt %d/%d)",
                file_path,
                bucket.name,
                blob_name,
                attempt + 1,
                max_retries,
            )
            blob.upload_from_filename(file_path, timeout=UPLOAD_TIMEOUT_SECONDS)

            if verify_gcs_upload(client, bucket, blob_name):
                logging.info("Upload verified: %s", blob_name)
                os.remove(file_path)
                logging.info("Deleted local file: %s", file_path)
                return True

            logging.warning("Verification failed for %s, retrying...", blob_name)
        except Exception as exc:
            logging.error("Upload failed for %s: %s", file_path, exc)

        time.sleep(UPLOAD_RETRY_DELAY_SECONDS)

    logging.error("Giving up upload for %s after %d attempts.", file_path, max_retries)
    return False


def main():
    args = parse_args()

    try:
        validate_months(args.months)
    except ValueError as exc:
        logging.error(str(exc))
        sys.exit(1)

    targets = build_targets(args)
    os.makedirs(args.download_dir, exist_ok=True)

    logging.info("Selected targets: %s", targets)
    logging.info("Selected months: %s", args.months)

    client, bucket = create_gcs_client_and_bucket(args.bucket_name)
    jobs = build_jobs(targets, args.months, args.download_dir)

    if not jobs:
        logging.error("No jobs generated. Check your taxi types/years/months arguments.")
        sys.exit(1)

    failed_downloads = []

    if not args.upload_only:
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
            download_results = list(executor.map(download_job, jobs))

        failed_downloads = [
            job["file_name"]
            for job, success in zip(jobs, download_results)
            if not success
        ]
    else:
        missing_files = [job["file_path"] for job in jobs if not os.path.exists(job["file_path"])]
        if missing_files:
            logging.error("Missing local files for upload-only mode:")
            for missing in missing_files:
                logging.error("  - %s", missing)
            sys.exit(1)

    upload_candidates = [
        job for job in jobs if os.path.exists(job["file_path"])
    ]

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as executor:
        upload_results = list(
            executor.map(
                lambda job: upload_job(job, client, bucket),
                upload_candidates,
            )
        )

    failed_uploads = [
        job["file_name"]
        for job, success in zip(upload_candidates, upload_results)
        if not success
    ]

    if failed_downloads or failed_uploads:
        if failed_downloads:
            logging.error("Failed downloads: %s", ", ".join(failed_downloads))
        if failed_uploads:
            logging.error("Failed uploads: %s", ", ".join(failed_uploads))
        logging.error("Completed with failures.")
        sys.exit(1)

    logging.info("Completed successfully.")


if __name__ == "__main__":
    main()
