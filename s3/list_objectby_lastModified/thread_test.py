import boto3
import os
import csv
import logging
import sys
from datetime import date
from concurrent.futures import ThreadPoolExecutor
import threading
from multiprocessing import Pool

## Setup logging config
logging.basicConfig( 
    format='%(asctime)s %(levelname)s: %(message)s', 
    level=logging.INFO, handlers=[
        logging.FileHandler("s3_scan_log.txt"),
        logging.StreamHandler(sys.stdout)
        ]
    )

## Variables
check_date: date = date(2018, 12, 31)

def scan_s3_buckets():
    s3 = boto3.resource('s3')
    old_data_buckets = []

    def scan_bucket(bucket):
        objects = list(bucket.objects.all())
        if not objects:
            logging.info('%s is empty, skipping... \n', bucket.name)
            return

        for obj in objects:
            lstmod = obj.last_modified.date() 
            if lstmod > check_date:
                old_data_buckets.append(bucket.name)
                logging.info('bucket: %s has old data, adding to list... \n', bucket.name)
                break

    threads = []
    for bucket in s3.buckets.all():
        t = threading.Thread(target=scan_bucket, args=(bucket,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return old_data_buckets

# Method to create subdirectories and write object data to CSV files
def write_csv(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = bucket.objects.all()
    
    ## Set defaults for object/csv/data
    object_count = 0
    csv_count = 1
    csv_data = []
    # Create Data directory if it doesn't exist
    if not os.path.exists('Data'):
        os.makedirs('Data')
    
    # Create subdirectory with bucket name
    subdirectory = os.path.join('Data', bucket.name)
    if not os.path.exists(subdirectory):
        os.makedirs(subdirectory)
        logging.info('Creating directory for bucket: %s', subdirectory)
        
    # Iterate through objects and write data to CSV
    for obj in objects:
                    
        ## increase object counter
        object_count += 1 
            
        ## Convert last_modified time to year-month-day format
        lstmod = obj.last_modified.date()                                    

        ## Conditional check for object lastmodified date being over 4 years old
        if lstmod > check_date:                 
                
            ## define variables for data rows
            csv_data.append([bucket.name, obj.key, obj.size, obj.last_modified, obj.storage_class])
            
            ## check if object count is at or below 1k
            if object_count % 1000 == 0:
                
                ## set csv_file name variable
                csv_file = os.path.join(subdirectory, f'{bucket.name}_{csv_count}.csv')
                                        
                ## Open CSV in write mode
                with open (csv_file, 'w', newline='') as file:
                    csv_writer = csv.writer(file)
                    header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']   
                    csv_writer.writerow(header)
                    csv_writer.writerows(csv_data)
                    
                    logging.info('Writing file: %s', csv_file)
                    
                    ##Increment csv count, clear data
                    csv_count += 1
                    csv_data = []  
                            
    
    
    ## Create next csv file
    if csv_data:
        csv_file = os.path.join(bucket_dir, f'{bucket.name}_{csv_count}.csv') 
        with open (csv_file, 'w', newline='') as file:
                    csv_writer = csv.writer(file)
                    header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']
                    csv_writer.writerow(header)
                    csv_writer.writerows(csv_data)
                    
                    logging.info('Writing file: %s \n', csv_file)

# Main method to execute the multithreaded S3 bucket scanner
def main():
    old_data_buckets = scan_s3_buckets()
    
    with ThreadPoolExecutor() as executor:
        for bucket_name in old_data_buckets:
            executor.submit(write_csv, bucket_name)
    
    print('Scanning and CSV generation completed successfully.')

if __name__ == '__main__':
    main()