import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time


# Change this to your bucket name
BUCKET_NAME = "module_3_om"

# If you authenticated through the GCP SDK you can comment out these two lines
# Prefer using the GOOGLE_APPLICATION_CREDENTIALS env var if set, otherwise
# fall back to Application Default Credentials.
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# If no env var set, look for service account file in current directory
if not CREDENTIALS_FILE:
    possible_files = [f for f in os.listdir(".") if f.endswith(".json") and "project" in f.lower()]
    if possible_files:
        CREDENTIALS_FILE = possible_files[0]
        print(f"Using service account file: {CREDENTIALS_FILE}")

if CREDENTIALS_FILE and os.path.exists(CREDENTIALS_FILE):
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    try:
        client = storage.Client()
    except Exception as e:
        print(f"Error initializing GCS client: {e}")
        print("Please set GOOGLE_APPLICATION_CREDENTIALS env var or place a service account JSON file in the current directory")
        sys.exit(1)
# If commented initialize client with the following
# client = storage.Client(project='zoomcamp-mod3-datawarehouse')


# Define URLs and data for both green and yellow trip data for 2019-2020
# Using GitHub releases: https://github.com/DataTalksClub/nyc-tlc-data/releases
GITHUB_RELEASES_BASE = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DATA_TYPES = ["yellow", "green"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(args):
    data_type, year, month = args
    file_path = os.path.join(DOWNLOAD_DIR, f"{data_type}_tripdata_{year}-{month}.parquet")
    
    # Check if file already exists locally
    if os.path.exists(file_path):
        print(f"File already exists, skipping download: {file_path}")
        return (data_type, file_path)
    
    # Construct GitHub releases URL
    url = f"{GITHUB_RELEASES_BASE}/{data_type}/{year}-{month}/{data_type}_tripdata_{year}-{month}.parquet"

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return (data_type, file_path)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


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


def upload_to_gcs(args, max_retries=3):
    data_type, file_path = args
    file_name = os.path.basename(file_path)
    # Create folder structure: yellow_tripdata/ or green_tripdata/
    blob_name = f"{data_type}_tripdata/{file_name}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME}/{blob_name} (Attempt {attempt + 1})...")
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

    # Create list of (data_type, year, month) tuples for both green and yellow data across 2019-2020
    download_tasks = [(data_type, year, month) for data_type in DATA_TYPES for year in YEARS for month in MONTHS]

    print(f"Preparing to download {len(download_tasks)} files...")
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, download_tasks))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("\nAll files processed and verified.")
    print(f"Files organized in:")
    print(f"  gs://{BUCKET_NAME}/yellow_tripdata/")
    print(f"  gs://{BUCKET_NAME}/green_tripdata/")