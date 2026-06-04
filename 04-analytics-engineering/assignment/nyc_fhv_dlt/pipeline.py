import gzip
import io
import dlt
import requests
import pandas as pd

CHUNK_SIZE = 100_000

def get_urls():
    urls = []
    for month in range(1, 13):
        url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/"
            f"releases/download/fhv/"
            f"fhv_tripdata_2019-{month:02d}.csv.gz"
        )
        urls.append(url)
    return urls


@dlt.resource(
    name="fhv_trips",
    write_disposition="append",
    primary_key="file_url",
)
def fhv_trips():
    for url in get_urls():
        print(f"Loading: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with gzip.open(io.BytesIO(response.content), "rt") as f:
            for chunk in pd.read_csv(f, chunksize=CHUNK_SIZE):
                chunk.columns = [
                    c.strip().lower().replace(" ", "_") for c in chunk.columns
                ]
                chunk["file_url"] = url
                yield chunk.to_dict(orient="records")

        print(f"Done: {url}")


pipeline = dlt.pipeline(
    pipeline_name="fhv_pipeline",
    destination="bigquery",
    dataset_name="nyc_tlc",
)

if __name__ == "__main__":
    load_info = pipeline.run(
        fhv_trips(),
        loader_file_format="parquet"  # ← serializes to Parquet before uploading
    )
    print(load_info)