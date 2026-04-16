import os
from google.cloud import storage

def main():
    # 1. Configuration - CHANGE THESE
    BUCKET_NAME = "inde-wild-landing" 
    CSV_FILES = ["01-31-Jan-2026-Blinkit-Sales.csv", "01-31-Jan-2026-Zepto.csv", "1-31-Jan-Nykaa-online.csv","myntra-jan26.csv"] 

    # 2. Initialize the client
    # It automatically looks for the GOOGLE_APPLICATION_CREDENTIALS env var
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 3. Upload loop
    for file_name in CSV_FILES:
        if os.path.exists(file_name):
            blob = bucket.blob(file_name) # Name in GCS
            print(f"Uploading {file_name}...")
            blob.upload_from_filename(file_name) # Source on local
            print(f"Done: {file_name} is now in {BUCKET_NAME}")
        else:
            print(f"Error: {file_name} not found in current directory.")

if __name__ == "__main__":
    main()