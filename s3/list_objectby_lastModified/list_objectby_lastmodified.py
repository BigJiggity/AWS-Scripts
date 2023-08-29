#!/usr/bin/python3

import boto3
import logging
import csv
import os
from datetime import date

# Setup logging config
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

## Set date for how far back you want to check
CHECK_DATE: date = date(2020, 8, 1)

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
                logging.info("Getting data for bucket: %s", bucket.name)
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
                        if lstmod >= CHECK_DATE:                 
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
        
        logging.info("Script Complete!")