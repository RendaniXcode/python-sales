import boto3
import csv
from io import StringIO

# Initialize the S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.resource('dynamodb')
table = dynamodb_client.Table('Sales_db')

def lambda_handler(event, context):
    # Process each SQS message
    for record in event['Records']:
        try:
            # Extract S3 event details from the SQS message body
            message_body = record['body']
            s3_event = eval(message_body)  # Convert string message body to dictionary
            bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
            file_key = s3_event['Records'][0]['s3']['object']['key']
            
            # Fetch the CSV file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')
            
            # Read the CSV content
            csv_reader = csv.DictReader(StringIO(csv_content))
            
            # Insert each row into DynamoDB
            for row in csv_reader:
                # Prepare the item for DynamoDB
                dynamodb_item = {key: str(value) for key, value in row.items() if value is not None}
                
                # Insert the item into DynamoDB
                table.put_item(Item=dynamodb_item)
            
            print(f"Data from {file_key} has been successfully inserted into Sales_db.")
        
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            raise e
