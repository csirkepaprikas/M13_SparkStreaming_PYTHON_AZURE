import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Name of the container in your Azure Blob Storage
container_name = os.getenv("CONTAINER_NAME")

# Local root folder containing your weather data (with subfolders like year=..., month=..., day=...)
local_root_folder = "c:/data_eng/házi/6/m13sparkstreaming/hotel-weather/"

# Delay between days (in seconds)
delay_seconds = 15

# Create a BlobServiceClient using the connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a reference to the container
container_client = blob_service_client.get_container_client(container_name)

#MAIN LOGIC: Upload day-by-day with delay in between

# Walk through folders
for year in sorted(os.listdir(local_root_folder)):
    year_path = os.path.join(local_root_folder, year)
    if not os.path.isdir(year_path):
        continue  # Skip if not a directory

    for month in sorted(os.listdir(year_path)):
        month_path = os.path.join(year_path, month)
        if not os.path.isdir(month_path):
            continue

        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue

            print(f"Uploading data for: {day_path}")

            # Upload all .parquet files for the current day (no delay between files)
            for filename in os.listdir(day_path):
                if filename.endswith(".parquet"):
                    local_file_path = os.path.join(day_path, filename)

                    # Build the relative blob path (preserving folder structure inside 'hotel-weather/')
                    relative_path = os.path.relpath(local_file_path, local_root_folder)
                    blob_path = f"hotel-weather/{relative_path.replace(os.sep, '/')}"

                    print(f"Uploading: {blob_path}")

                    # Open the file and upload it to Azure Blob Storage
                    with open(local_file_path, "rb") as file:
                        container_client.upload_blob(name=blob_path, data=file, overwrite=True)

            print(f"Finished uploading files for {day_path}.\n")

            # Wait before moving to the next day
            time.sleep(delay_seconds)

print("All weather data successfully uploaded day by day!")
