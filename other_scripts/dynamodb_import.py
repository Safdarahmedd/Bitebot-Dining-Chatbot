# Batch import into dynamodb
import boto3
import json
from decimal import Decimal
from datetime import datetime

# Set up AWS credentials and region
aws_access_key_id = 'ACCESS_KEY'
aws_secret_access_key = 'SECRET_KEY'
region_name = 'us-east-1'
out_file = r"C:/Search_Engines/Crawler/seed_files/yelp_scrap_api.json"
# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          region_name=region_name)

# Define the table name
table_name = 'yelp_restaurants'

# Load JSON data from file
def json_decoder(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

with open(out_file, 'r') as json_file:
    items = json.load(json_file, parse_float=Decimal, parse_constant=json_decoder)

# Get DynamoDB table
table = dynamodb.Table(table_name)

for item in items:
    item['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

# Batch write items into DynamoDB table
with table.batch_writer() as batch:
    for item in items:
        batch.put_item(Item=item)

print(f"All items have been imported into the '{table_name}' table.")
