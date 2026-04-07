import json
import os
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("=== metadata extractor invoked ===")

    for record in event['Records']:
        # Parse the SNS message string as JSON
        sns_message = json.loads(record['Sns']['Message'])
        
        # Loop through the S3 records inside the SNS message
        for s3_record in sns_message['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']
            size = s3_record['s3']['object']['size']
            event_time = s3_record['eventTime']

            # Required log format
            print(f"[METADATA] File: {key}")
            print(f"[METADATA] Bucket: {bucket}")
            print(f"[METADATA] Size: {size} bytes")
            print(f"[METADATA] Upload Time: {event_time}")

            # Build metadata dict
            metadata = {
                "file": key,
                "bucket": bucket,
                "size": size,
                "upload_time": event_time
            }

            # Get filename (e.g. "uploads/test.jpg" -> "test")
            filename = os.path.splitext(key.split('/')[-1])[0]

            # Write metadata to S3
            s3.put_object(
                Bucket=bucket, 
                Key=f"processed/metadata/{filename}.json",
                Body=json.dumps(metadata), 
                ContentType='application/json'
            )

    return {'statusCode': 200, 'body': 'metadata extracted'}