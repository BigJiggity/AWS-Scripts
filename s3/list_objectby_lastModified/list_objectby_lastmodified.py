#!/usr/bin/python3

import boto3
import logging
import csv
import os
from datetime import date
import sys

## Setup logging config
logging.basicConfig( 
    format='%(asctime)s %(levelname)s: %(message)s', 
    level=logging.INFO, handlers=[
        logging.FileHandler("s3_scan_log.txt"),
        logging.StreamHandler(sys.stdout)
        ]
    )

## Set date for how far back you want to check
CHECK_DATE: date = date(2020, 8, 1)

## Check if Data Directory exists, if not create data folder
if not os.path.exists("Data"):
    os.makedirs("Data")

## Folder for file check
folder_path = "Data/"

## List of buckets to skip during iteration
skip_buckets = ["analytics-emr-runtime",]
   
    ## Add previously processed buckets to the skip_bucket list
for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        skip_buckets.append(os.path.splitext(file_name)[0])
logging.info("front loading previously generated csv files into skip buckets")

def get_bucket_data(buckets: list) -> None:
        """ 
        Method to get data from each bucket and store in a CSV file
        Args: buckets (list): List of buckets to get data from
        """
        
        ## Iterate/Process through each bucket
        for bucket in buckets:
                logging.info("Getting data for bucket: %s", bucket.name)
                
                # Get the object data in the bucket
                objects = bucket.objects.all() 
                
                ## Convert creation_date time to year-month-day format
                bcdate = bucket.creation_date.date()
                
                ## Logic for logging purposes, skipping buckets if they exist in skip_buckets list
                if bucket.name in skip_buckets:
                    logging.info("Bucket %s has been processed, Skipping Bucket... \n", bucket.name)
                
                ## Check if bucket was created in the last 3yrs, if yes, add to the skip buckets variable so they are not processed.
                elif bcdate >= CHECK_DATE:
                    skip_buckets.append(bucket.name)
                    logging.info("Checking creation date: %s: - %s is newer than 3yrs - adding to skip_buckets list... \n", bucket.creation_date, bucket.name)     
                
                ## Check if there are any objects in the bucket, if not, add bucket to skip_buckets list
                elif len(list(objects)) == 0:
                    skip_buckets.append(bucket.name)
                    logging.info("Bucket %s has zero objects, added to skip bucket list \n", bucket.name)
                
                ## Check if bucket is not in the skip_bucket list, process object data in bucket
                elif bucket.name not in skip_buckets:
                    logging.info("Processing Bucket: %s \n", bucket.name)
                    
                    ## Create a CSV file for each bucket
                    csv_file = f"{bucket.name}.csv"
                        
                    ## Open CSV in write mode
                    with open('Data/%s' %csv_file, 'w', newline='') as file:
                        csv_writer = csv.writer(file)
                        
                        ## define values for header row
                        header: list[str] = ['File_Name', 'Last_Modified_Date',
                            'File Size', 'Storage Class', 'Owner']  
                       
                        ## Write Header to csv
                        csv_writer.writerow(header)                 
                            
                        ## Set base counts for objects/csv's
                        object_count = 0
                        csv_count = 1
                        
                        ## Iterate/Process through Objects
                        for obj in bucket.objects.all():
                                    
                            ## Convert last_modified time to year-month-day format
                            lstmod = obj.last_modified.date()                                    

                            ## Conditional check for object lastmodified date being 3+ years old
                            if lstmod <= CHECK_DATE:                 
                                    
                                ## define variables for data rows
                                data: list = ['%s' %obj.key, '%s' %obj.last_modified, '%s' %
                                        obj.size, '%s' %obj.storage_class, '%s' %obj.owner]
                                
                                ## Write Data to csv
                                csv_writer.writerow(data)
                                
                                ## Increment the object counter
                                object_count += 1
                                
                                ## Check object count, if count reaches 10000, create a new CSV file
                                if object_count == 10000:
                                    file.close()
                                    
                                    ## Create new CSV file with incremented name
                                    csv_file_name = f"{bucket.name}_{csv_count}.csv"
                                    csv_file = open(csv_file_name, 'w', newline='')
                                    csv_writer = csv.writer(csv_file)
                                    
                                    ## Write Header row
                                    csv_writer.writerow(header)
                                    
                                    ## Reset the object count and increment the csv count
                                    object_count = 0
                                    csv_count += 1
                                
                        ## Close the CSV file for the current bucket
                        csv_file.close()
                                

                            
                                
                                    
                                # ## Check if the CSV file has reached the row limit
                                # if file.tell() >= 10000:
                                    
                                #     ## Close the current CSV file
                                #     file.close()
                                #     logging.info("%s has reached 10000 rows, closing file... \n" %file)
                                    
                                #     ## Increment the file name number
                                #     file_number = int(csv_file.split('.')[0].split('_')[1]) + 1
                                    
                                #     ## Create a new CSV file with the incremented number
                                #     new_csv_file = f"{bucket.name}_{file_number}.csv"
                                #     logging.info("Creating new csv to continue scan: %s \n" %new_csv_file)
                                    
                                #     ## Open the new CSV file in write mode
                                #     file = open(new_csv_file, 'w', newline='')
                                #     writer = csv.writer(file)
                                    
                                #     ## Write the header row for the new CSV file
                                #     writer.writerow(header)                   
                                                        
if __name__ == "__main__":
        
        try:
        ## Create resource
                s3 = boto3.resource('s3')
        except Exception as e:
                logging.error("Error creating S3 resource. Error: %s \n", e)
                
        buckets = s3.buckets.all()
        
        get_bucket_data(buckets)
        
        logging.info("Script Complete! \n")