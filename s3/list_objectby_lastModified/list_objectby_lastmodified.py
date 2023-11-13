#!/usr/bin/python3

import boto3
import logging
import csv
import sys
import os

from datetime import date

## Setup logging config
logging.basicConfig( 
    format='%(asctime)s %(levelname)s: %(message)s', 
    level=logging.INFO, handlers=[
        logging.FileHandler("s3_scan_log.txt"),
        logging.StreamHandler(sys.stdout)
        ]
    )

## Set date for how far back you want to check
CHECK_DATE: date = date(2018, 12, 31)

## Check if Data Directory exists, if not create data folder
data_dir = 'Data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    
## List of buckets to skip during iteration
skip_buckets = ["analytics-emr-runtime","cengage-analytics-platform-prod","cl-analytics-prod","cengage-s3-access-logs","cl-gale-chef-receiver-prod"]


## Begin main definition
def get_bucket_data(buckets: list) -> None:
        """ 
        Method to get data from each bucket and store in a CSV file
        Args: buckets (list): List of buckets to get data from
        """
        
        ## Iterate/Process through each bucket
        for bucket in buckets:
            
            if sum(1 for _ in bucket.objects.all()) == 0:
            skip_buckets.append(bucket_name)
            return

            ## check for bucket conditions to skip
            folder_path = os.path.join(data_dir, bucket.name)
            
            if os.path.exists(folder_path): 
                
                logging.info("folder for %s already exists, skipping...", bucket.name)
                skip_buckets.append(bucket.name)

            else:
                ## Check if bucket is not in the skip_bucket list, process object data in bucket
                if bucket.name not in skip_buckets:
                    
                    logging.info("Getting data for bucket: %s", bucket.name)
                    
                    ## Create directories for CSV's
                    bucket_dir = os.path.join(data_dir, bucket.name)
                    if not os.path.exists(bucket_dir):
                        os.makedirs(bucket_dir)
                    logging.info("created: %s... ", bucket_dir)
                    logging.info("Processing Bucket...: %s", bucket.name)
                    
                    ## Set defaults for object/csv/data
                    object_count = 0
                    csv_count = 1
                    csv_data = []

                    ## Iterate/Process through Objects
                    for obj in bucket.objects.all():
                   
                        ## Iterate Objects
                        object_count += 1 
                            
                        ## Convert last_modified time to year-month-day format
                        lstmod = obj.last_modified.date()                                    

                        ## Conditional check for object lastmodified date being 3+ years old
                        if lstmod <= CHECK_DATE:                 
                                
                            ## define variables for data rows
                            csv_data.append([bucket.name, obj.key, obj.size, obj.last_modified, obj.storage_class])
                            
                            ## check if object count is at or below 10k
                            if object_count % 1000 == 0:
                                
                                ## Create a CSV file for each bucket
                                csv_file = os.path.join(bucket_dir, f'{bucket.name}_{csv_count}.csv')
                                                        
                                ## Open CSV in write mode
                                with open (csv_file, 'w', newline='') as file:
                                    csv_writer = csv.writer(file)
                                    
                                    ## define values for header row
                                    header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']  
                                        
                                    ## Write Header to csv
                                    csv_writer.writerow(header)
                                
                                    ## Write Data to csv
                                    csv_writer.writerows(csv_data)
                                    
                                    logging.info('Writing file: %s \n', csv_file)
                                    
                                    ##Increment csv count, clear data
                                    csv_count += 1
                                    csv_data = []  
                                            
                    ## define values for header row
                    header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']

                    ## Create next csv file
                    if csv_data:
                        csv_file = os.path.join(bucket_dir, f'{bucket.name}_{csv_count}.csv') 
                        with open (csv_file, 'w', newline='') as file:
                                    csv_writer = csv.writer(file)
                                    csv_writer.writerow(header)
                                    csv_writer.writerows(csv_data)
                                    
                                    logging.info('Writing file: %s \n', csv_file)
                            
if __name__ == "__main__":
        
        try:
                ## Create resource
                s3 = boto3.resource('s3')
        except Exception as e:
                logging.error("Error creating S3 resource. Error: %s \n", e)
                sys.exit(1)
        buckets = s3.buckets.all()
        
        get_bucket_data(buckets)
        
        logging.info("Script Complete! \n")