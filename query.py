import requests
import boto3
import os
import json
import time
from requests.exceptions import HTTPError

# Egress API and AWS S3 Configuration
EGRESS_API_BASE_URL = "https://api.egress.com/"
WORKSPACE_ID = "your_workspace_id"
AUTH_TOKEN = "your_auth_token"
HEADERS = {"Authorization": f"Bearer {AUTH_TOKEN}"}
S3_BUCKET = 'your_s3_bucket_name'
REGION_NAME = 'your_aws_region'
AWS_ACCESS_KEY_ID = 'your_aws_access_key_id'
AWS_SECRET_ACCESS_KEY = 'your_aws_secret_access_key'
TRACKING_FILE = "downloaded_files.json"

# Initialize AWS S3 Client
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION_NAME)

def get_downloaded_files():
    try:
        with open(TRACKING_FILE, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def save_downloaded_files(files):
    with open(TRACKING_FILE, "w") as file:
        json.dump(files, file)

def make_api_request(url, method='get', data=None, stream=False):
    for attempt in range(5):  # Retry up to 5 times
        try:
            if method == 'get':
                response = requests.get(url, headers=HEADERS, json=data, stream=stream)
            response.raise_for_status()  # Raise an exception for 4XX/5XX errors
            return response
        except HTTPError as e:
            if response.status_code == 429:  # Rate limit error
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise e  # Re-raise for other HTTP errors
        except Exception as e:
            print(f"Error making API request: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff for non-HTTP errors
    return None  # Return None if all retries fail

def list_files_from_workspace():
    url = f"{EGRESS_API_BASE_URL}/workspaces/{WORKSPACE_ID}/files"
    response = make_api_request(url)
    if response:
        return response.json()["files"]
    return []

def download_and_upload_file(file_id, file_name):
    download_url = f"{EGRESS_API_BASE_URL}/files/{file_id}/download"
    response = make_api_request(download_url, stream=True)
    if response:
        s3_key = f"uploads/{file_name}"
        try:
            s3_client.upload_fileobj(response.raw, S3_BUCKET, s3_key)
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
    return False

def process_files():
    downloaded_files = get_downloaded_files()
    files = list_files_from_workspace()

    for file in files:
        file_id = file["id"]
        file_name = file["name"]
        last_modified = file["lastModified"]

        if file_id not in downloaded_files or downloaded_files[file_id] < last_modified:
            print(f"Processing: {file_name}")
            if download_and_upload_file(file_id, file_name):
                downloaded_files[file_id] = last_modified

    save_downloaded_files(downloaded_files)

if __name__ == "__main__":
    process_files()
