import boto3
import pprint
import io
from datetime import date, datetime, timedelta

## Set up pretty print for easier reading
pp = pprint.PrettyPrinter(indent=4)

# Instantiate session with local creds file in ~/.aws/credentials
#session = boto3.Session(profile_name='default')

# Create client object to interact with s3 endpoints
#s3client = session.client('s3')

s3=boto3.resource('s3')

# Print out bucket names
for bucket in s3.buckets.all():
    print("Bucket name ", bucket.name)
    page=boto3.client('s3')
# bucket_name = s3client.list_buckets() 
  #Create reusable Paginator
    s3_paginator = page.get_paginator('list_objects_v2')
    #Create Page Iterator from paginator
    page_iterator = s3_paginator.paginate(Bucket=bucket.name)
    #Filter results on lastModified date
    filtered_iterator = page_iterator.search("Contents[?to_string(LastModified)>='\"2023-03-01 00:00:00+00:00\"'].Key")

 #Show Contents
for object in filtered_iterator:
  pp.pprint(object)


# # for bucketname in client.buckets.all():
# #   print(bucket.name)
# # s3_iterator = s3_paginator.paginate(Bucket=bucketname)
# # filtered_iterator = s3_iterator.search(
# #     "Contents[?to_string(LastModified)>='\"2020-08-18 00:00:00+00:00\"'].Key"
# # )
# # for key_data in filtered_iterator:
# #     print(key_data)
