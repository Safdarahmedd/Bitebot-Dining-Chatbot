import json
import datetime
import boto3
from boto3.dynamodb.conditions import Key
import uuid
import requests
from requests_aws4auth import AWS4Auth
import random
""" prev state"""
es = boto3.client('es',region_name='us-east-1')
sqs_url = 'SQS_URL'
# Hardcoded for testing
key = 'ACCESS_KEY'
secret= 'SECRET_KEY'

#awsauth = AWS4Auth(boto3.Session().get_credentials().access_key, boto3.Session().get_credentials().secret_key,'us-east-1','es')
awsauth = AWS4Auth(key,secret,'us-east-1','es')
os_host = 'OS_URL'
index = 'restaurants'
es_url = os_host + '/' + index + '/_search'
dynamodb = boto3.resource('dynamodb',region_name='us-east-1')

class EST(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=-4)

    def dst(self, dt):
        return datetime.timedelta(0)


def lambda_handler(event, context):
    ctime = datetime.datetime.now(EST())
    date_str = str(ctime.hour)+':'+str(ctime.minute)
    print('LFO-Invoked:', event)
    
    body = event['body']  
    
    # Parse the JSON string from the body
    body_json = json.loads(body) 
    
    # Access the 'messages' key and extract the text
    messages = body_json['messages']
    
    # Assuming there's only one message in the list
    message = messages[0]['unstructured']['text']

     
    lex_tags_client = boto3.client('lex-runtime', region_name='us-east-1',
                                   aws_access_key_id= 'ACCESS_KEY', aws_secret_access_key='SECRET_KEY')
                                   
        
    response = lex_tags_client.post_text(
        botName = 'BOT_NAME',
        botAlias = 'BOT_ALIAS',
        userId = 'USER_ID',
        inputText=message)
    print('LFO Response: ', response["message"])
    
    if(response["message"] == "What location do you want suggestions for?"):
        
        recomm = get_recommendation(message)
        print("Reco from older state",recomm)
        if(recomm != 'No-State'):    
            return {
                'statusCode': 200,
                'headers': {
                'Access-Control-Allow-Headers': 'Content-Type, Origin, X-Auth-Token',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({
                "messages": [
                {
                "type": "unstructured",
                "unstructured": {
                    "id": "1",
                    "text": recomm,
                    "timestamp": date_str
                }
                }
            ]
        })
        }
        
    return {
    'statusCode': 200,
    'headers': {
        'Access-Control-Allow-Headers': 'Content-Type, Origin, X-Auth-Token',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    },
    'body': json.dumps({
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "1",
                    "text": response["message"],
                    "timestamp": date_str
                }
            }
        ]
    })
    }
    
def get_recommendation(email):
    # get previous cuisine
    cuisine = fetch_cuisine(email)
    if cuisine is None: return 'No-State'
    # get new recommendation based on the old one
     # Get resturant ids
    restaurant_ids = get_restaurants_ids(cuisine)
 
    # Get restaurant details
    restaurants = get_restaurant_details(restaurant_ids)
 
    # Construct message body
    message = construct_message_body(restaurants)

    return message


def construct_message_body(restaurants):
    formatted_details = "Based on your previous search, here are the top suggestions for you in Manhattan: <br><br>"
    for restaurant in restaurants:
        formatted_details += f"Name: {restaurant.get('name', 'N/A')}  "
        formatted_details += f"Address: {restaurant.get('display_address', 'N/A')}  "
        #formatted_details += f"Cuisine: {restaurant.get('cuisine', 'N/A')}  "
        #formatted_details += f"Rating: {restaurant.get('rating', 'N/A')}  "
        formatted_details += "<br>"  # Add a newline between restaurants
    formatted_details += "<br>Please enter the location for which you want recommendations to continue\n\n\n"
    print("Prev - Recommendation: ", formatted_details)
    return formatted_details
    
    

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

    # Execute the search query
    #search_results = es.search(index="restaurants", body=query)

    # Extract restaurant IDs from the search results
    #restaurant_ids = [hit['_id'] for hit in search_results['hits']['hits']]

    # Randomly select 5 restaurant IDs for this demo
    n_restaurant_ids = random.sample(restaurants_id_list, min(num_entries, len(restaurants_id_list)))
    print("res_ids: ", n_restaurant_ids)
    return n_restaurant_ids    




def fetch_cuisine(email):
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    
    # Define the table name
    table_name = 'restaurant_state'
    table = dynamodb.Table(table_name)
    try:
        # Query the DynamoDB table for the cuisine associated with the email
        response = table.query(
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            }
        )
        
        print("prev cuisine response: ", response)
        
        #response_data = json.loads(response)

        # Extract cuisine from the 'Items' list if it exists
        if 'Items' in response and len(response['Items']) > 0:
            cuisine = response['Items'][0]['cuisine']
            return cuisine
        else:
            print("No cuisine found in the response.")
    except Exception as e:
        print("Error fetching cuisine:", e)
        return None
    return None