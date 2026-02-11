import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa


# Change this to your bucket name
BUCKET_NAME = "kestra-bucket-dezoomcamp2026-485910"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/workspaces/dezoomcamp2026/google-creds.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='zoomcamp-mod3-datawarehouse')

try:
    YEAR = sys.argv[1]
    COLOR = sys.argv[2]
except IndexError:
    print("Please enter a Year and taxi color")
    sys.exit()

BASE_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{COLOR}_tripdata_{YEAR}-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(
        DOWNLOAD_DIR, f"{COLOR}_tripdata_{YEAR}-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
                           
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def normalize_schema(file_path):

    column = 'airport_fee' if COLOR == 'yellow' else 'ehail_fee'

    try:
        table = pq.read_table(file_path)
        table = table.set_column(
        table.schema.get_field_index(column),
        column,
        pc.cast(table[column], pa.float64())
        )
        pq.write_table(table, file_path)
        print(f"{file_path} - field updated")
    except Exception as err:
        print(err)
     

def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    
    
    # blob_name = os.path.basename(file_path)
    blob_name = os.path.join(f'{COLOR}_taxi', os.path.basename(file_path)).replace(os.sep, '/')
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)
    

    for attempt in range(max_retries):
        try:
            print(
                f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))
    
    for file in file_paths:
        normalize_schema(file)

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(
            None, file_paths))  # Remove None

    # schema = pq.read_schema('./yellow_tripdata_2019-05.parquet')
    # print(schema)

    print("All files processed and verified.")
