import boto3
import pprint
import csv
from datetime import date, datetime, timedelta

## Set up pretty print for easier reading
pp = pprint.PrettyPrinter(indent=4)

## Use Commented lines for SSO/Session logins for programatic access otherwise script uses default creds in ~/.aws/credentials

#session = boto3.Session(profile_name='default')

## Create client object to interact with s3 endpoints
#s3client = session.client('s3')

#Create resource
s3=boto3.resource('s3')

## Print out bucket names
for bucket in s3.buckets.all():
    print("Bucket name ", bucket.name)
    
    ## Setup Paginate to iterate through buckets/objects
    page=boto3.client('s3')
    ## Create reusable Paginator
    s3_paginator = page.get_paginator('list_objects_v2')
    ## Create Page Iterator from paginator to list buckets
    bucket_iterator = s3_paginator.paginate(Bucket=bucket.name)
    ## Filter results on lastModified date
    filtered_iterator = bucket_iterator.search("Contents[?to_string(LastModified)>='\"2019-08-01 00:00:00+00:00\"'].Key")
    
## Show Contents
for object in filtered_iterator:
  pp.pprint(object)

  ## output to csv
  header = ['Bucket Name', 'Object Name', 'Last Modified Date']
  data = [bucket.name, object]
  ## create csv file
  with open('s3_old_data.csv', 'w', encoding='UTF8', newline='') as f:
    ## Define File 
    writer = csv.writer(f)
    ## write the header
    writer.writerow(header)
    ## write the data
    writer.writerows(data)  