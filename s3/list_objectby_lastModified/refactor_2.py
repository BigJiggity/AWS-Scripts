import boto3
import logging
import csv
import sys
import os
from datetime import date
from concurrent.futures import ThreadPoolExecutor
import time

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
checkdate_1: date = date(2018, 12, 31)
checkdate_2: date = date(2020, 1, 1)
state_file = 'state.txt'

# Check if Data Directory exists, if not create data folder
data_dir = 'Data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

def process_bucket(bucket):
    """
    Method to process data for a single bucket and store it in a CSV file
    Args:
        bucket (boto3.resources.factory.s3.Bucket): Bucket to process
    """
    # Check for bucket conditions to skip
    folder_path = os.path.join(data_dir, bucket.name)
    
    # Check if there is a state file
    if not check_state_file(state_file):
        # If no state file, start from the beginning
        start_bucket = None
        start_object = None
    else:
        # If state file exists, read the last known bucket and object
        start_bucket, start_object = read_state_file(state_file)
    
    # Check if the current bucket is the last known bucket
    if start_bucket and bucket.name == start_bucket:
        # If yes, start from the last known object
        objects = bucket.objects.filter(Marker=start_object)
    else:
        # If no, start from the beginning of the bucket
        objects = bucket.objects.all()
            
    # Create directories for CSVs
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    logging.info("Created: %s... ", folder_path)

    # Set defaults for object/csv/data
    object_count = 0
    csv_count = 1
    csv_data = []

    # Iterate/Process through Objects
    for obj in objects:

        # Iterate Objects
        object_count += 1

        # Convert last_modified time to year-month-day format
        lstmod = obj.last_modified.date()

        # Conditional check for object last modified date being between 2020 and 2018
        if lstmod > checkdate_1:

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

    # Update the state file with the current bucket and object
    update_state_file(state_file, bucket.name, obj.key)

    # Create next CSV file
    if csv_data:
        csv_file = os.path.join(folder_path, f'{bucket.name}_{csv_count}.csv')
        with open(csv_file, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(header)
            csv_writer.writerows(csv_data)

            logging.info('Writing file: %s \n', csv_file)


def check_state_file(state_file):
    if not os.path.exists(state_file):
        return False
    return True

def read_state_file(state_file):
    try:
        with open(state_file, 'r') as file:
            last_bucket, last_object = file.read().split(',')
            return last_bucket, last_object
    except FileNotFoundError:
        return None, None

def update_state_file(state_file, bucket, obj):
    with open(state_file, 'w') as file:
        file.write(f'{bucket},{obj}')

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