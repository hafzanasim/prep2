# data_storage.py

import boto3
import json
from google.cloud import storage

# Function to store data in AWS S3
#def store_data_s3(data, bucket_name, file_name):
    #s3 = boto3.client('s3')
    #try:
        #s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data))
        #print(f"Data saved to S3 at {file_name}")
    #except Exception as e:
        #print(f"Error saving data to S3: {e}")

# Function to store data in Google Cloud Storage
def store_data_gcs(data, bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    try:
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        print(f"Data saved to GCS at {file_name}")
    except Exception as e:
        print(f"Error saving data to GCS: {e}")
