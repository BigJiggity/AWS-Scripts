import boto3
import pprint
import csv
import os
from datetime import date, datetime, timedelta

## Set up pretty print for easier reading
pp = pprint.PrettyPrinter(indent=4)

## Check if Data Directory exists, if not create data folder
if not os.path.exists("Data"):
    os.makedirs("Data")
    
## Use Commented lines for SSO/Session logins for programatic access otherwise script uses default creds in ~/.aws/credentials

session = boto3.Session()

## Create client object to interact with s3 endpoints
s3client = session.client('s3')

#Create resource
s3=boto3.resource('s3')

response = s3client.list_object_versions(
    Bucket='testicle-bucket-2'
)

## Print out bucket names
for bucket in s3.buckets.all():
    pp.pprint("Bucket name: {}".format(bucket.name))
    
    # All object data example
    pp.pprint("Object Info: {}".format(response))
    
    ## Setup Paginate to iterate through buckets/objects
    # page=boto3.client('s3')
    # ## Create reusable Paginator
    # s3_paginator = page.get_paginator('list_objects_v2')
    # ## Create Page Iterator from paginator to list buckets
    # bucket_iterator = s3_paginator.paginate(Bucket=bucket.name)
    # ## Filter results on lastModified date
    # filtered_iterator = bucket_iterator.search("Contents[?to_string(LastModified)>='\"2019-08-01 00:00:00+00:00\"'].Key")
    
## Iterate through buckets, creating a csv per bucket
    # for b in bucket.name:  
    #   ## create csv file
    #   with open('Data/%s_s3_old_data.csv' %bucket.name, 'w', encoding='UTF8', newline='') as f:
    #     ## write the file 
    #     writer = csv.writer(f) 

    # with open('Data/%s_s3_old_data.csv' %bucket.name, 'a', encoding='UTF8',) as f:
    #     writer = csv.writer(f)
    #   ## define variables for header row
    #     header = ['Bucket_Name', 'Object_Name', 'Last_Modified_Date',] 
    #     ## write the header
    #     writer.writerow(header)  
    #     ## Iterate the data
    #     for o in filtered_iterator: 
    #       ## define variables for data rows
    #       data = ['%s' %bucket.name, '%s' %o,]
    #       ## Write data to file
    #       writer.writerow(data)
          
        
 