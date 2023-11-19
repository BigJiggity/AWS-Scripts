import boto3
import logging
import csv
import sys
import os
from datetime import date
from concurrent.futures import ThreadPoolExecutor
import time
import json

# Setup logging config
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("s3_scan_log.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Set date for how far back you want to check
CHECK_DATE: date = date(2018, 12, 31)

# Check if Data Directory exists, if not create data folder
data_dir = 'Data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# List of buckets to skip during iteration
skip_buckets = ["analytics-emr-runtime", "cengage-analytics-platform-prod", "cl-analytics-prod",
                "cengage-s3-access-logs", "cl-gale-chef-receiver-prod"]


def process_bucket(bucket):
    """
    Method to process data for a single bucket and store it in a CSV file
    Args:
        bucket (boto3.resources.factory.s3.Bucket): Bucket to process
    """
    # Load the state file
    state = load_state()

    bucket_name = bucket.name

    # Get the bucket name and last object name from the state
    last_bucket_name = state.get('bucket_name')
    last_object_name = state.get('last_object_name')

    # Check for bucket conditions to skip
    folder_path = os.path.join(data_dir, bucket.name)

    # Check if bucket is not in the skip_bucket list, process object data in bucket
    if bucket.name not in skip_buckets:
        logging.info("Processing Bucket...: %s", bucket.name)

        # Create directories for CSVs
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        logging.info("Created: %s... ", folder_path)

        # Set defaults for object/csv/data
        object_count = 0
        csv_count = 1
        csv_data = []

        # Iterate/Process through Objects
        for obj in bucket.objects.filter(Prefix=last_object_name):

            # Iterate Objects
            object_count += 1

            # Convert last_modified time to year-month-day format
            lstmod = obj.last_modified.date()

            # Conditional check for object last modified date being 4+ years old or if object exists in state file
            if lstmod > CHECK_DATE:

                # Define variables for data rows
                csv_data.append([bucket.name, obj.key, obj.size, obj.last_modified, obj.storage_class])

                # Check if object count is at or below 1k
                if object_count % 1000 == 0:

                    # Create a CSV file for each bucket
                    csv_file = os.path.join(folder_path, f'{bucket.name}_{csv_count}.csv')

                    # Open CSV in write mode
                    with open(csv_file, 'w', newline='') as file:
                        csv_writer = csv.writer(file)

                        # Define values for header row
                        header = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']

                        # Write Header to CSV
                        csv_writer.writerow(header)

                        # Write Data to CSV
                        csv_writer.writerows(csv_data)

                        logging.info('Writing file: %s \n', csv_file)

                        # Increment CSV count, clear data
                        csv_count += 1
                        csv_data = []

                        # Update the last object name in the state
                        state['last_object_name'] = obj.key
                        state['last_bucket_name'] = bucket.name

        # Save the updated state
        save_state(state)

        # Create next CSV file
        if csv_data:
            csv_file = os.path.join(folder_path, f'{bucket.name}_{csv_count}.csv')
            with open(csv_file, 'w', newline='') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(header)
                csv_writer.writerows(csv_data)

                logging.info('Writing file: %s \n', csv_file)


def load_state():
    # Load the state from a file or create a new state if the file doesn't exist
    try:
        with open('state.json', 'r') as file:
            state = json.load(file)
    except FileNotFoundError:
        state = {'last_bucket_name': '', 'last_object_name': ''}

    return state


def save_state(state):
    # Save the state to a file
    with open('state.json', 'w') as file:
        json.dump(state, file)


if __name__ == "__main__":
    try:
        # Create resource
        s3 = boto3.resource('s3')
    except Exception as e:
        logging.error("Error creating S3 resource. Error: %s \n", e)
        sys.exit(1)

    buckets = s3.buckets.all()

    # Multithreading to speed up S3 bucket scanning and writing of the CSV files
    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        executor.map(process_bucket, buckets)
    end_time = time.time()

    logging.info("Script Complete! \n")
    logging.info("Execution Time: %s seconds", end_time - start_time)