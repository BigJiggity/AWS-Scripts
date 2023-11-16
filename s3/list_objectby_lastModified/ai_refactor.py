import boto3
import csv
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def thread_buckets():
    # Create an S3 resource
    s3 = boto3.resource('s3')
    
    # Get all buckets
    buckets = list(s3.buckets.all())
    
    # Create a thread pool executor
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Iterate through each bucket
        for bucket in buckets:
            logging.info('Starting thread for bucket: %s \n', bucket.name)
            executor.submit(scan_buckets, bucket)
            logging.info('Finished thread \n')
    
    logging.info("Script completed.")

def scan_buckets(bucket):
    logging.info("Beginning function scan_buckets")
    
    # Get all objects in the bucket
    objects = list(bucket.objects.all())
    
    # Set variables/lists
    object_count = 0
    csv_count = 1
    csv_data = []
    check_date = date(2018, 12, 31)
    
    if not objects:
        logging.info('%s is empty, skipping... \n', bucket.name)
        return
    
    # Iterate through each object
    for obj in objects:
        # Increase object counter
        object_count += 1
        
        # Check if the object is over 4 years old
        if obj.last_modified < check_date:
            # Define variables for data rows
            csv_data.append([bucket.name, obj.key, obj.size, obj.last_modified, obj.storage_class])
            logging.info('Data written to csv_data list: %s', csv_data)
            
            # Check if object count is at or below 1k
            if object_count % 1000 == 0:
                # Write the object details to the CSV file
                csv_file = open(f"{bucket.name}_{csv_count}.csv", "w", newline="")
                csv_writer = csv.writer(csv_file)
                header = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']
                csv_writer.writerow(header)
                csv_writer.writerows(csv_data)
                logging.info('Writing file: %s', csv_file)
                
                # Increment csv count, clear data
                csv_count += 1
                csv_data = []
    
    header = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']
    
    # Create next csv file
    if csv_data:
        csv_file = open(f"{bucket.name}_{csv_count}.csv", "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header)
        csv_writer.writerows(csv_data)
        logging.info('Writing file: %s \n', csv_file)
    
    logging.info(f"Scanning of bucket {bucket.name} completed. \n")

# Run the function
thread_buckets()