#!/usr/bin/python3

import boto3
import logging
import csv
import os
from datetime import date

# Setup logging config
logging.basicConfig(filename="s3_scanlog.txt", format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

## Set date for how far back you want to check
CHECK_DATE: date = date(2020, 8, 1)

# List of buckets to skip during iteration
skip_buckets = ['analytics-emr-runtime', 'alsv2-production', 'alteryx-prod', 'aplia-logs-prod', 'aplia-materials-prod', 'aplia-platform-prod', 'aplia-prod-sqlbackup',
                'aplia-publishing-prod', 'aplia-rawdatadownload-prod', 'aplia-reciept-prod', 'apliacoursespub', 'apliaprod-itemregelb', 'apliaprod-plat-int', 'apliaq4prod',
                'av-archive-backup', 'becaa-prod', 'bigdataDev', 'bigdataProd', 'cassandra-prod-ops', 'cassandra-unloader-analytics-prod',
                'ccp-video-transcoding-prod', 'cengage-analytics-platform-airflow', 'cengage-analytics-platform-cdn-prod', 'cengage-analytics-platform-prod']

## Check if Data Directory exists, if not create data folder
if not os.path.exists("Data"):
    os.makedirs("Data")

def get_bucket_data(buckets: list) -> None:
        """ 
        Method to get data from each bucket and store in a CSV file
        Args: buckets (list): List of buckets to get data from
        """
        ## Iterate through each bucket
        for bucket in buckets:
                logging.info("Getting data for buckets...")
                
                ## Convert creation_date time to year-month-day format
                bcdate = bucket.creation_date.date()
                
                ## Logic for logging purposes, skipping buckets if they exist in skip_buckets list
                if bucket.name in skip_buckets:
                    logging.info("Bucket %s has been processed, Skipping Bucket... \n", bucket.name)
                
                ## Check if bucket was created in the last 3yrs, if yes, add to the skip buckets variable so they are not processed.
                elif bcdate >= CHECK_DATE:
                    skip_buckets.append(bucket.name)
                    logging.info("Checking creation date for bucket: %s - %s: - bucket is newer than 3yrs",bucket.name, bucket.creation_date)    
                    logging.info("Updating skiped buckets list... \n")
                    
                ## Check if bucket is not in the skip_bucket list, process object data in bucket
                elif bucket.name not in skip_buckets:
                    logging.info("Processing Bucket: %s \n", bucket.name)                    
                    ## Create a CSV file for each bucket
                    csv_file = open('Data/%s.csv' %bucket.name, 'w', newline='')
                    csv_writer = csv.writer(csv_file)

                    ## define values for header row
                    header: list[str] = ['File_Name', 'Last_Modified_Date',
                            'File Size', 'Storage Class', 'Owner']
                    
                    ## Write Header to csv
                    csv_writer.writerow(header)

                    ## List objects inside the bucket
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

if __name__ == "__main__":
        
        try:
        ## Create resource
                s3 = boto3.resource('s3')
        except Exception as e:
                logging.error("Error creating S3 resource. Error: %s", e)
                
        buckets = s3.buckets.all()
        
        get_bucket_data(buckets)
        
        logging.info("Script Complete! \n")