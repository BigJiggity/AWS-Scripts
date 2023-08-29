import boto3
import pprint
import csv
import os
from datetime import date

## Set up pretty print for easier reading
pp = pprint.PrettyPrinter(indent=4)

## Check if Data Directory exists, if not create data folder
if not os.path.exists("Data"):
    os.makedirs("Data")

########################################################################
# Set Session or use 'default' AWS credentials and create resource
########################################################################

# Use Commented lines for SSO/Session logins for programatic access otherwise script uses default creds in ~/.aws/credentials
# session = boto3.Session()

# Create client object to interact with s3 endpoints
# s3client = session.client('s3')

# Create resource
s3 = boto3.resource('s3')

########################################################################
# Main
########################################################################

## Set date for how far back you want to check
check_date = date(2020, 8, 1)

## Get all S3 buckets
buckets = s3.buckets.all()


## Iterate through each bucket
for bucket in buckets:
        
        ## Create a CSV file for each bucket
        csv_file = open('Data/%s.csv' %bucket.name, 'w', newline='')
        csv_writer = csv.writer(csv_file)

        ## define values for header row
        header = ['File_Name', 'Last_Modified_Date',
                'File Size', 'Storage Class', 'Owner']

        ## Write Header to csv
        csv_writer.writerow(header)
        
        ## List objects inside the bucket
        for obj in bucket.objects.all():
            ## Convert last_modified time to year-month-day format
            lstmod = obj.last_modified.date()

            ## Conditional check for object lastmodified date being 3+ years old
            if lstmod >= check_date: 
                            
                    ## define variables for data rows
                    data = ['%s' %obj.key, '%s' %obj.last_modified, '%s' %
                            obj.size, '%s' %obj.storage_class, '%s' %obj.owner]

                    ## Write Data to csv
                    csv_writer.writerow(data)
