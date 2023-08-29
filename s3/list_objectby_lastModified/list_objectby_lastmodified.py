import boto3
import pprint
import csv
import os
from datetime import  date

## Set up pretty print for easier reading
pp = pprint.PrettyPrinter(indent=4)

## Check if Data Directory exists, if not create data folder
if not os.path.exists("Data"):
    os.makedirs("Data")
    
########################################################################
# Set Session or use 'default' AWS credentials and create resource
########################################################################

## Use Commented lines for SSO/Session logins for programatic access otherwise script uses default creds in ~/.aws/credentials
#session = boto3.Session()

## Create client object to interact with s3 endpoints
#s3client = session.client('s3')

## Create resource
s3=boto3.resource('s3')

########################################################################
# Main
########################################################################

## Set date for how far back you want to check
check_date = date(2020, 8, 28)
   
## Get all S3 buckets
buckets = s3.buckets.all()

## Iterate through each bucket
for bucket in buckets:
    print(f"Bucket Name: {bucket.name}")
    
    ## List objects inside the bucket
    for obj in bucket.objects.all():
       
        ## Convert last_modified time to year-month-day format
        lstmod = obj.last_modified.date()

        #Conditional check for object lastmodified date being 3+ years old
        if lstmod >= check_date:
            print(f"Object Key: {obj.key}", f"Last Modified: {obj.last_modified}", f"Object Size: {obj.size}", f"Storage Class: {obj.storage_class}", f"Owner: {obj.owner}")
        
########################################################################
# CSV Generator
########################################################################
                
# Iterate through buckets, creating a csv per bucket
    for b in buckets:  
      
      ## create csv file
      with open('Data/%s_s3_old_data.csv' %bucket.name, 'w', encoding='UTF8', newline='') as f:
        
        ## write the file 
        writer = csv.writer(f) 
    with open('Data/%s_s3_old_data.csv' %bucket.name, 'a', encoding='UTF8',) as f:
        writer = csv.writer(f)
      
        ## define variables for header row
        header = ['Object_Name', 'Last_Modified_Date', 'Object Size', 'Storage Class', 'Owner'] 
        
        ## write the header
        writer.writerow(header)  
        
        ## Iterate the data and assign variables
        for o in buckets.objects.all(): 
            obj_name = obj.key
            obj_mod_date = obj.last_modified
            obj_size = obj.size
            obj_sc = obj.storage_class
            obj_owner = obj.owner
          
            ## define variables for data rows
            data = ['%s' %obj_name, '%s' %obj_mod_date, '%s' %obj_size, '%s' %obj_sc, '%s' %obj_owner]
          
            ## Write data to file
            writer.writerow(data)
    
 
 