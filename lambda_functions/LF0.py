import json
import datetime
import boto3
from boto3.dynamodb.conditions import Key
import uuid


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
    print('LFO Response: ', response)
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
