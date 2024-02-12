import json
import boto3

# Create S3 and Kinesis clients
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis', region_name='us-east-1')

def lambda_handler(event, context):
    print(json.dumps(event))

    # Extract S3 bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key_name = event['Records'][0]['s3']['object']['key']

    # Retrieve object content from S3
    response = s3.get_object(Bucket=bucket_name, Key=key_name)
    data_string = response['Body'].read().decode('utf-8')

    # Create payload
    payload = {'data': data_string}

    # Call send_to_kinesis function
    send_to_kinesis(payload, key_name)

def send_to_kinesis(payload, partition_key):
    # Prepare parameters for putting a record into Kinesis stream
    params = {
        'Data': json.dumps(payload),
        'PartitionKey': partition_key,
        'StreamName': 'learn-data-stream'
    }

    # Put record into Kinesis stream
    try:
        response = kinesis.put_record(**params)
        print(response)
    except Exception as e:
        print(f"Error putting record into Kinesis: {e}")

# Uncomment the line below if you want to test the code locally
# lambda_handler({'Records': [{'s3': {'bucket': {'name': 'your-bucket-name'}, 'object': {'key': 'your-object-key'}}}]}, None)
