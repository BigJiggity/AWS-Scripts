import boto3
import os
import csv
import logging
import sys
from datetime import date
import threading

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

 ## Create Data directory if it doesn't exist
if not os.path.exists('Data'):
        os.makedirs('Data')

def scan_buckets(buckets):
    # s3 = boto3.resource('s3')
 
    ## set object counter / set csv count
    object_count = 0
    csv_count = 1
    
    ## Iterate through buckets, set objects list    
    for bucket in buckets:
        objects = list(bucket.objects.all())
                                    
        if not objects:
            logging.info('%s is empty, skipping... \n', bucket.name)
            
        else:
            for obj in objects:
                
                ## Set last modified date variable and check object date against set check_date variable
                lstmod = obj.last_modified.date()
                
                ## Increase object count
                object_count += 1
                
                # Create subdirectory with bucket name
                subdirectory = os.path.join('Data', bucket.name)
                if not os.path.exists(subdirectory):
                    os.makedirs(subdirectory)
                    logging.info('Creating directory for bucket: %s', subdirectory)
                    
                if lstmod < check_date:
            
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
            csv_file = os.path.join(subdirectory, f'{bucket.name}_{csv_count}.csv') 
            with open (csv_file, 'w', newline='') as file:
                        csv_writer = csv.writer(file)
                        header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']
                        csv_writer.writerow(header)
                        csv_writer.writerows(csv_data)
                        
                        logging.info('Writing file: %s \n', csv_file)
                    
                
        threads = []
        for bucket in s3.buckets.all():
            t = threading.Thread(target=scan_buckets, args=(bucket,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

if __name__ == "__main__":
        
    try:
            ## Create resource
            s3 = boto3.resource('s3')
    except Exception as e:
            logging.error("Error creating S3 resource. Error: %s \n", e)
            sys.exit(1)
    buckets = s3.buckets.all()
    scan_buckets(buckets)
    logging.info("Script Complete! \n")
    