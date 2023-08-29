import boto3
import pprint
import csv
import os
from datetime import datetime, timezone, timedelta
from dateutil import parser

## Set up pretty print for easier reading
pp = pprint.PrettyPrinter(indent=4)

## Check if Data Directory exists, if not create data folder
if not os.path.exists("Data"):
    os.makedirs("Data")
    
## FINISHED: Use Commented lines for SSO/Session logins for programatic access otherwise script uses default creds in ~/.aws/credentials
#session = boto3.Session()
## FINISHED: Create client object to interact with s3 endpoints
#s3client = session.client('s3')

## FINISHED: Create client and resource
s3r=boto3.resource('s3')
s3=boto3.client('s3')

##################################
# Option 1
##################################

# Get the current date
current_date = datetime.now(timezone.utc).timestamp()
print(current_date)

# ## FINISHED: Gather Buckets

bkts = s3.list_buckets()

## FINISHED: Output the bucket names
for bucket in bkts['Buckets']:
     mybucket = (bucket["Name"])



# Get the list of objects in the bucket
objects = s3.list_objects_v2(Bucket=mybucket)['Contents']

## Iterate over each object and check if it hasn't been modified in the past 3 years

for obj in objects:
    last_modified = obj['LastModified']
    print(timedelta(3*365))
    if current_date - timedelta(days=3*365) > last_modified:
        print(obj['Key'], obj['last_modified'], obj['size'], obj['storage_class'])   

##################################################################

##################################
# Option 2
##################################

# ## FINISHED: Get a bucket, and list all objects in the bucket
# bn = s3r.Bucket(bucket["Name"])
# files_in_bucket = list(bn.objects.all())

    
# ## Test for last_modified
# for object in files_in_bucket:
#     lastmod = object.last_modified
#     print(lastmod)
#     print(check_date)

#     #### TypeError: '<=' not supported between instances of 'datetime.datetime' and 'str' ####
#     if lastmod >= check_date: 
#         filtered_obj = lastmod
    
#     ## Print filtered results
#     for f in filtered_obj:    
#         print(object.key, object.last_modified, object.size, object.storage_class)

################################################
   
########################################
# Option 3
########################################

# # Create reusable Paginator
# s3_paginator = s3.get_paginator(bn.objects.all())

# # Create Page Iterator from paginator to list buckets
# bucket_iterator = s3_paginator.paginate(Bucket=bucket["Name"])
 
# ## Filter results on lastModified date
# filtered_iterator = bucket_iterator.search("Contents[?to_string(LastModified)>='\"2019-08-01 00:00:00+00:00\"'].Key")

# for object in filtered_iterator:
#     print(object.key, object.last_modified, object.size, object.storage_class)
   

##########################################
# CSV Generator
##########################################
                
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
 