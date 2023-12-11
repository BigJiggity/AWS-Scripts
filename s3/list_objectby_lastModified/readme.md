<h1>S3 Bucket Scanner and CSV Writer</h1>
-------------------------------------------
<h3>Introduction</h3>
This code is a Python script that scans all the objects in an Amazon S3 bucket and writes the metadata of the objects to CSV files. It uses the Boto3 library to interact with the Amazon S3 service and the ThreadPoolExecutor class to speed up the scanning process by utilizing multiple threads.

<h3>Key Concepts</h3>

Before diving into the code, let's understand some key concepts:

**Amazon S3:** Amazon Simple Storage Service (S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. It allows you to store and retrieve any amount of data from anywhere on the web. \
**Boto3:** Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) for Python. It allows Python developers to write software that makes use of services like Amazon S3, Amazon EC2, etc.\
**CSV:** CSV stands for Comma-Separated Values. It is a simple file format used to store tabular data (numbers and text) in plain text. Each line of the file represents a row of the table, and the values are separated by commas.
Multithreading: Multithreading is a technique in which multiple threads of execution share the same memory space and can run concurrently. It allows for parallel execution of tasks, which can improve performance and efficiency.\

<h3>Code Structure</h3>

The code is structured into several sections:

**Importing Libraries:** The necessary libraries, such as boto3, logging, csv, sys, os, and time, are imported.\
**Logging Configuration:** The logging configuration is set up to log messages to both a file (s3_scan_log.txt) and the console.\
**Date Configuration:** Two dates, checkdate_1 and checkdate_2, are set to define the range of object last modified dates to consider during the scanning process. These dates can be modified to suit your specific requirements.\
**Data Directory Creation:** The code checks if a directory named "Data" exists. If not, it creates the directory to store the CSV files.\
**Bucket Processing:** The process_bucket function is defined to process the objects of a single bucket and store their metadata in a CSV file. It takes a bucket object as an argument.\
**State File Handling:** The check_state_file, read_state_file, and update_state_file functions are defined to handle the state file, which keeps track of the last processed bucket and object. This allows the script to resume from where it left off in case of interruptions.\
**Main Execution:** The main execution block creates an S3 resource using the boto3 library. It then retrieves all the buckets using the buckets.all() method. The process_bucket function is executed concurrently for each bucket using the ThreadPoolExecutor class. The execution time is logged at the end.\
