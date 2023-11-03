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
CHECK_DATE: date = date(2019, 11, 1)

## Check if Data Directory exists, if not create data folder
data_dir = 'Data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    
## List of buckets to skip during iteration
skip_buckets = ["analytics-emr-runtime","cengage-analytics-platform-prod",]
   


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
            
            logging.info("Getting data for bucket: %s \n", bucket.name)
        
            ## check for bucket conditions to skip
            if os.path.exists(f'Data/{bucket.name}') and bcdate >= CHECK_DATE and len(list(objects)) == 0:
                logging.info("Skipping Bucket %s sinec it's either been processed already, has 0 objects, or is older than 3 years", bucket.name)
                skip_buckets.append(bucket.name)
            
            ## Check if bucket is not in the skip_bucket list, process object data in bucket
            elif bucket.name not in skip_buckets:
                logging.info("Processing Bucket...: %s \n", bucket.name)
                
                ## Create directories for CSV's
                bucket_dir = os.path.join(data_dir, bucket.name)
                if not os.path.exists(bucket_dir):
                    os.makedirs(bucket_dir)
                logging.info("created: %s... ", bucket_dir)
            
                ## Create a CSV file for each bucket
                csv_file = os.path.join(bucket_dir, f'{bucket.name}.csv')
                logging.info("Current file is: %s", csv_file)    
                
                ## Open CSV in write mode
                with open (csv_file, 'w', newline='') as file:
                    csv_writer = csv.writer(file)
                    
                    ## define values for header row
                    header: list[str] = ['File_Name', 'Last_Modified_Date',
                        'File Size', 'Storage Class', 'Owner']  
                    
                    ## Write Header to csv
                    csv_writer.writerow(header)                
                    
                    ## Iterate/Process through Objects
                    for obj in bucket.objects.all():
                                
                        ## Convert last_modified time to year-month-day format
                        lstmod = obj.last_modified.date()                                    

                        ## Conditional check for object lastmodified date being 3+ years old
                        if lstmod >= CHECK_DATE:                 
                                
                            ## define variables for data rows
                            data: list = ['%s' %obj.key, '%s' %obj.last_modified, '%s' %
                                    obj.size, '%s' %obj.storage_class, '%s' %obj.owner]
                            
                            logging.debug("Writer file is: %s", csv_file)

                            ## Set base count for objects
                            object_count = 0

                            ## Set base count for csv's
                            csv_count = 0
                            
                            ## Write Data to csv
                            csv_writer.writerow(data)
                        
                            ## Increment the object counter
                            object_count += 1

                            ## Check object count, if count reaches 10000, create a new CSV file
                            if object_count == 10000:
                                file.close()
                                
                                ## increment the csv count
                                csv_count += 1

                                ## Create new CSV file with incremented name
                                csv_file_name = os.path.join(bucket_dir, f'{bucket.name}_{csv_count}.csv')
                                with open (csv_file_name, 'w', newline='') as new_file:
                                    csv_writer = csv.writer(new_file)
                                    logging.info("Created next csv file: %s \n", csv_file_name)
                                
                                    ## Write Header row
                                    csv_writer.writerow(header)       
                            
                    ## Close the CSV file for the current bucket
                    file.close()

                    ## Reset the object/csv count
                    object_count = 0
                    csv_count = 0
                                                        
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