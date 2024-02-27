import json
import boto3
from elasticsearch import Elasticsearch
import requests
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key
from variables import *
from botocore.exceptions import ClientError
import random

sender_email = 'YOUR_EMAIL'
# Initialize SES client
ses = boto3.client('ses',region_name='us-east-1')
sqs = boto3.client('sqs',region_name='us-east-1')
dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
es = boto3.client('es',region_name='us-east-1')
sqs_url = 'SQS_URL'
# Hardcoded for testing
key = 'ACCESS_KEY'
secret= 'SECRET_KEY'

#awsauth = AWS4Auth(boto3.Session().get_credentials().access_key, boto3.Session().get_credentials().secret_key,'us-east-1','es')
awsauth = AWS4Auth(key,secret,'us-east-1','es')
os_host = 'ES_URL'
index = 'restaurants'
es_url = os_host + '/' + index + '/_search'

def get_restaurants_ids(cuisine, num_entries=5):
    # Define Elasticsearch query to retrieve restaurants by cuisine
    query = {
        "query": {
            "match": {
                "cuisine": cuisine
            }
        }
    }
    
    headers = { "Content-Type": "application/json" }

    # Make the signed HTTP request
    r = requests.get(es_url, auth=awsauth, headers=headers, data=json.dumps(query))
    
    # # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }
    print("r.text", r.text)
    restaurants_list = json.loads(r.text)['hits']['hits']
    print("res_json_list: ", restaurants_list)
    restaurants_id_list = [x['_source']['id'] for x in restaurants_list]


    # Randomly select 5 restaurant IDs for this demo
    n_restaurant_ids = random.sample(restaurants_id_list, min(num_entries, len(restaurants_id_list)))
    print("res_ids: ", n_restaurant_ids)
    return n_restaurant_ids


def get_restaurant_details(restaurant_ids):
    table_name = 'yelp_restaurants'
    expression_attribute_names = {
    '#old_name': 'name',
    '#new_name': 'restaurant_name'
    }

    # Batch get items from DynamoDB table
    response = dynamodb.batch_get_item(
        RequestItems={
            table_name: {
                'Keys': [{'id': restaurant_id} for restaurant_id in restaurant_ids],
                'ProjectionExpression': 'id, #old_name, display_address, cuisine, rating,#new_name',
                'ExpressionAttributeNames': expression_attribute_names
            }
        }
    )

    # Extract restaurant details from the response
    restaurants = response['Responses'][table_name]
    print("Details:",restaurants)
    return restaurants    



def handle_sqs_message(response):
    message = response.get('Messages', [])[0]
    
    # Extract message attributes
    message_attributes = message.get('MessageAttributes', {})
            
    # Extract email and cuisine attributes
    email = message_attributes.get('email', {}).get('StringValue', 'N/A')
    cuisine = message_attributes.get('Cuisine', {}).get('StringValue', 'N/A').capitalize()
            
    print("Received message:")
    print(f"Email: {email}")
    print(f"Cuisine: {cuisine}")

    # Delete the message from the queue
    sqs.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=message['ReceiptHandle']
    )
    return cuisine,email

def poll_sqs():
    sqs_message = sqs.receive_message(
        QueueUrl=sqs_url,  
        MaxNumberOfMessages=1,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=10
    )
    messages = sqs_message.get('Messages', [])
    for message in messages:
            # Extract message attributes
            message_attributes = message.get('MessageAttributes', {})
            for key, value in message_attributes.items():
                print(f"Attribute {key}: {value['StringValue']}")
    
    #sqs.delete_message(
     #           QueueUrl=sqs_url,
     #           ReceiptHandle=message['ReceiptHandle']
      #      )                
    
    return sqs_message

def send_email(recipient, body_text):
    
    # Specify email subject and body
    subject = 'Bitebot Restaurant Recommendations'
    
    # Send email
    try:
        response = ses.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [recipient]
            },
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body_text}}
            }
        )
        print("Email sent! Message ID:", response['MessageId'])
        return {
            'statusCode': 200,
            'body': json.dumps('Success')
        }
    except ClientError as e:
        print("Error:", e.response['Error']['Message'])
        return {
            'statusCode': 500,
            'body': json.dumps('Failed')
        }

        
def construct_message_body(restaurants):
    formatted_details = "Here are the top suggestions for you in Manhattan: \n\n"
    for restaurant in restaurants:
        formatted_details += f"Name: {restaurant.get('name', 'N/A')}\n"
        formatted_details += f"Address: {restaurant.get('display_address', 'N/A')}\n"
        formatted_details += f"Cuisine: {restaurant.get('cuisine', 'N/A')}\n"
        formatted_details += f"Rating: {restaurant.get('rating', 'N/A')}\n"
        formatted_details += "\n"  # Add a newline between restaurants
    return formatted_details


def lambda_handler(event, context):
   
 # Poll sqs to get the next event
 sqs_message = poll_sqs()
 print("Trigger-SQS-message: ", sqs_message)
 # get the details
 cuisine,email = handle_sqs_message(sqs_message)

 
 # Get resturant ids
 restaurant_ids = get_restaurants_ids(cuisine)
 
 # Get restaurant details
 restaurants = get_restaurant_details(restaurant_ids)
 
 # Construct message body
 message = construct_message_body(restaurants)
 
 # store for recomendations
 store_info(email,cuisine)
 
 # Send the email and return the status
 return send_email(email,message)
 
 
def store_info(email, cuisine):
    #if not isinstance(email, str):
    print(type(email))
    print("casting email")
    email_str = str(email)
    print(type(email_str))
    print("Email save:", email_str)
    print("Cusine save", cuisine)
    table_name = 'restaurant_state'
    table = dynamodb.Table(table_name)
    item = {
        'email':  email_str,
        'cuisine': cuisine
    }
    
    try:
        # Check if an item with the same email exists
        response = table.get_item(Key={'email': email})
        if 'Item' in response:
            # If item exists, delete it
            table.delete_item(Key={'email': email})
            print("Existing item with email", email, "deleted.")
        # Put item into DynamoDB table
        table.put_item(TableName=table_name, Item=item)
        print("Information stored successfully.")
    except Exception as e:
        print("Error storing information:", e)