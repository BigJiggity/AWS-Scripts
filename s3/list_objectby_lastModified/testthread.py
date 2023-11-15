import boto3
import csv
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def thread_buckets():
    
    
    
    # Create an S3 resource
    s3 = boto3.resource('s3')
    
    # Get all buckets
    global buckets
    buckets = list(s3.buckets.all())
    
    # Create a thread pool executor
    executor = ThreadPoolExecutor(max_workers=50)
    
    # Iterate through each bucket
    for bucket in buckets:
        executor.submit(scan_buckets, bucket)
    
    # Wait for all threads to complete
    executor.shutdown(wait=True)
    
    logging.info("Script completed.")

def scan_buckets(bucket):
    
    logging.info("begining bucket scan... %s", bucket.name)
    
    ## set variables/lists
    object_count = 0
    csv_count = 1
    csv_data = []
    check_date: date = date(2018, 12, 31)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    for bucket in buckets:
       
        # Get all objects in the bucket
        objects = list(bucket.objects.all())
        
        # Iterate through each object
        for obj in objects:
            
            ## increase object counter
            object_count += 1 
        
            # Check if the object is over 4 years old
            if obj.last_modified > check_date:
                
                ## define variables for data rows
                csv_data.append([bucket.name, obj.key, obj.size, obj.last_modified, obj.storage_class])
                
                logging.info('data witten to csv_data list: %s', csv_data)
                
                ## check if object count is at or below 1k
                if object_count % 1000 == 0:
                
                    # Write the object details to the CSV file
                    csv_file = open(f"{bucket.name}_{csv_count}.csv", "w", newline="")
                    csv_writer = csv.writer(csv_file)
                    header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']
                    csv_writer.writerow(header)
                    csv_writer.writerows(csv_data)

                    logging.info('Writing file: %s', csv_file)
                        
                    ##Increment csv count, clear data
                    csv_count += 1
                    csv_data = []  
    
        header: list[str] = ['Bucket Name', 'File_Name', 'File Size', 'Last_Modified_Date', 'Storage Class']             
    
        ## Create next csv file
        if csv_data:
            csv_file = open(f"{bucket.name}_{csv_count}.csv", "w", newline="")
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)
            csv_writer.writerows(csv_data)
            
            logging.info('Writing file: %s \n', csv_file)
        
        logging.info(f"Scanning of bucket {bucket.name} completed. \n")

# Run the function
thread_buckets()