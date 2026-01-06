import boto3
import os
from dotenv import load_dotenv

# This line finds the .env file and "pours" it into your system's memory
load_dotenv()


def load_json_to_s3(local_file_path, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    
    print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
    s3_client.upload_file(local_file_path, bucket_name, s3_key)