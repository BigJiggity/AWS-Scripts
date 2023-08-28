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
#session = boto3.Session()
## Create client object to interact with s3 endpoints
#s3client = session.client('s3')

#Create client and resource
s3r=boto3.resource('s3')
s3=boto3.client('s3')



## Gather Buckets
bkts = s3.list_buckets()

## Output the bucket names
print('Existing buckets:')
for bucket in bkts['Buckets']:
    pp.pprint(bucket["Name"])
    mybucket = (bucket["Name"])
    
    ## Get a bucket, and list all objects in the bucket
    bn = s3r.Bucket(bucket["Name"])
    files_in_bucket = list(bn.objects.all())
     
    ## Get object Attributes: name, last modified date, object size
    for b in files_in_bucket:
        obj_names = [f.key for f in files_in_bucket]
        for n in obj_names:
            name = n

        obj_lastmod = [f.last_modified for f in files_in_bucket]
        for m in obj_lastmod:
            lastmod = m
            
        obj_sizes = [f.size for f in files_in_bucket]
        for s in obj_sizes:
            size = s
                
        print(name, size, lastmod)



    
     ## Create reusable Paginator
    ## Create Page Iterator from paginator to list buckets
    #s3_paginator = s3.get_paginator('list_objects_v2')
    # bucket_iterator = s3_paginator.paginate(Bucket=bucket["Name"])
 


    # ## Filter results on lastModified date
    # filtered_iterator = bucket_iterator.search("Contents[?to_string(LastModified)>='\"2019-08-01 00:00:00+00:00\"'].Key")
    
    # for object in filtered_iterator:
    #     print(object)
   
                
# # Iterate through buckets, creating a csv per bucket
#     for b in bucket["Name"]:  
#       ## create csv file
#       with open('Data/%s_s3_old_data.csv' %bucket["Name"], 'w', encoding='UTF8', newline='') as f:
#         ## write the file 
#         writer = csv.writer(f) 

#     with open('Data/%s_s3_old_data.csv' %bucket["Name"], 'a', encoding='UTF8',) as f:
#         writer = csv.writer(f)
#       ## define variables for header row
#         header = ['Bucket_Name', 'Object_Name', 'Last_Modified_Date',] 
#         ## write the header
#         writer.writerow(header)  
#         ## Iterate the data
#         for o in filtered_iterator: 
#           ## define variables for data rows
#           data = ['%s' %bucket["Name"], '%s' %o,]
#           ## Write data to file
#           writer.writerow(data)
    
    #######################################################      




#     ## Gather object Metadata    
#     metadata = s3.list_objects_v2(Bucket=bucket["Name"])
#     # All object data example
#     pp.pprint("Object Info: {}".format(metadata))   
 