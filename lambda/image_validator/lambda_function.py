import json
import os
import boto3

s3 = boto3.client('s3')

VALID_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

def is_valid_image(key):
    """check if the file has a valid image extension."""
    _, ext = os.path.splitext(key.lower())
    return ext in VALID_EXTENSIONS

def lambda_handler(event, context):
    print("=== image validator invoked ===")

    for record in event['Records']:
        # Parse the SNS message string as JSON
        sns_message = json.loads(record['Sns']['Message'])
        
        for s3_record in sns_message['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']

            if is_valid_image(key):
                print(f"[VALID] {key} is a valid image file")
                
                # Get filename (e.g. "uploads/test.jpg" -> "test.jpg")
                filename = key.split('/')[-1]
                
                # Copy object to processed/valid/
                s3.copy_object(
                    Bucket=bucket, 
                    Key=f"processed/valid/{filename}",
                    CopySource={'Bucket': bucket, 'Key': key}
                )
            else:
                print(f"[INVALID] {key} is not a valid image type")
                # Raise exception to trigger DLQ
                raise ValueError(f"Invalid file type: {key}")

    return {'statusCode': 200, 'body': 'validation complete'}