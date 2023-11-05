#!/usr/bin/python3

from ast import While
from operator import truediv
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
CHECK_DATE: date = date(2019, 11, 1)

## Check if Data Directory exists, if not create data folder
data_dir = 'Data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    
## List of buckets to skip during iteration
skip_buckets = ["analytics-emr-runtime","cengage-analytics-platform-prod",]

## Begin main definition
def get_bucket_data(buckets: list) -> None:
        """ 
        Method to get data from each bucket and store in a CSV file
        Args: buckets (list): List of buckets to get data from
        """
        
        ## Iterate/Process through each bucket
        for bucket in buckets:
    
            # Get the object data in the bucket
            objects = bucket.objects.all() 
            
            ## Convert creation_date time to year-month-day format
            bcdate = bucket.creation_date.date()
            
            ## check for bucket conditions to skip
            folder_path = os.path.join(data_dir, bucket.name)
            
            if os.path.exists(folder_path): 
                
                logging.info("folder for %s already exists, skipping...", bucket.name)
                skip_buckets.append(bucket.name)
                
            # elif bcdate >= CHECK_DATE: 

            #     logging.info("bucket %s isn't old enough, skipping...", bucket.name)
            #     skip_buckets.append(bucket.name)
            
            elif len(list(objects)) == 0:
                
                logging.info("There are no objects in bucket %s, skipping...", bucket.name)
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
                            csv_data.append([obj.key, obj.last_modified, obj.size, obj.storage_class, obj.owner])
                            
                            ## check if object count is at or below 10k
                            if object_count % 10000 == 0:
                                
                                ## Create a CSV file for each bucket
                                csv_file = os.path.join(bucket_dir, f'{bucket.name}_{csv_count}.csv')
                                                        
                                ## Open CSV in write mode
                                with open (csv_file, 'w', newline='') as file:
                                    csv_writer = csv.writer(file)
                                    
                                    ## define values for header row
                                    header: list[str] = ['File_Name', 'Last_Modified_Date',
                                        'File Size', 'Storage Class', 'Owner']  
                                        
                                    ## Write Header to csv
                                    csv_writer.writerow(header)
                                
                                    ## Write Data to csv
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