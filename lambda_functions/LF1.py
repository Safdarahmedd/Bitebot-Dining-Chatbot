import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json
import re
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


"""  Passing message to SQS  """

def sendSQS(request_data):
    sqs_client = boto3.client(
        'sqs', aws_access_key_id='ACCESS_KEY', aws_secret_access_key='SECRET_KEY')

    location = request_data["Location"]
    cuisine = request_data["Cuisine"]
    number_of_people = request_data["Number_of_people"]
    dining_date = request_data["Dining_date"]
    dining_time = request_data["Dining_time"]
    email = request_data["Email"]

    message_attributes = {
        "location": {
            'DataType': 'String',
            'StringValue': location
        },
        "Cuisine": {
            'DataType': 'String',
            'StringValue': cuisine
        },
        "NumberOfPeople": {
            'DataType': 'Number',
            'StringValue': number_of_people
        },
        "Dining_date": {
            'DataType': 'String',
            'StringValue': dining_date
        },
        "Dining_time": {
            'DataType': 'String',
            'StringValue': dining_time
        },
        "Email": {
            'DataType': 'String',
            'StringValue': email
        }
    }
    body = ('Resturant Slots')

    response = sqs_client.send_message(
        QueueUrl='QUEUE_URL',  MessageAttributes=message_attributes, MessageBody=body)

    return



"""  Helper functions for Lex """


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


"""  Helper Functions for response validation """


def is_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def get_validation_response(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def date_validation(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False


def email_validation(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if(re.fullmatch(regex, email)):
        return True
    return False


def validate_dining_slots(location, cuisine, number_of_people, dining_date, dining_time, email):
    print("Validating dining suggestions..")
    # Locations
    locations = ['manhattan']
    cuisines = ['italian', 'indian', 'thai']
    if location is not None and location.lower() not in locations:
        return get_validation_response(False,
                                       'Location',
                                       'Currently we do not operate in {}, would you like to change your location?  '
                                       'We have extensive coverage of restaurants located in Manhattan'.format(location))

    # Cuisine
    if cuisine is not None and cuisine.lower() not in cuisines:
        return get_validation_response(False, 'Cuisine',
                                       'Currently we do not have data for restaurants with {} cuisine, do you want to try another cuisine?'
                                       ' We have a lot of options for Italian, Thai and Indian food'.format(cuisine))

    # Number of people
    if number_of_people is not None:
        number_of_people = is_int(number_of_people)
        if not 0 < number_of_people < 30:
            return get_validation_response(False, 'Number_of_people', '{} does not look like a valid number, '
                                           'please enter a number less than 30'.format(number_of_people))

    # Dining_date
    if dining_date is not None:
        if not date_validation(dining_date):
            return get_validation_response(False, 'Dining_date', 'I did not understand that, what date do you have in mind?')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return get_validation_response(False, 'Dining_date', 'Unfortunately, we can\'t travel back in time! You can pick a date from today onwards. What date do you have in mind?')

    # Dining_time

    if dining_time is not None:
        if len(dining_time) != 5:
            return get_validation_response(False, 'Dining_time', 'Please enter time in 24-hour format')

        hour, minute = dining_time.split(':')
        hour = is_int(hour)
        minute = is_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return get_validation_response(False, 'Dining_time', 'Please enter time in 24-hour format')

        # Edge case
        ctime = datetime.datetime.now()

        if datetime.datetime.strptime(dining_date, "%Y-%m-%d").date() == datetime.datetime.today():
            if (ctime.hour >= hour and ctime.minute > minute) or ctime.hour < hour or (ctime.hour == hour and minute <= ctime.minute):
                return get_validation_response(False, 'Dining_time', 'Please select a time in the future.')


    # Email
    if email is not None:
        if not email_validation(email):
            return get_validation_response(False, 'Email', '{} is not a valid email,'
                                           'please enter a valid email'.format(email))

    return get_validation_response(True, None, None)


""" Functions to handle intents """

def greeting_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Hi there, how can I help?'})


def thank_you_intent(intent_request):
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "Happy to help!"})


def dining_suggestions(intent_request):
    
    slots = get_slots(intent_request)
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    number_of_people = slots["Number_of_people"]
    dining_date = slots["Dining_date"]
    dining_time = slots["Dining_time"]
    email = slots["Email"]
    source = intent_request['invocationSource']

    request_data = {
        "Location": location,
        "Cuisine": cuisine,
        "Number_of_people": number_of_people,
        "Dining_date": dining_date,
        "Dining_time": dining_time,
        "Email": email
    }

    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {
    }
    output_session_attributes['requestData'] = json.dumps(request_data)

    if source == 'DialogCodeHook':
        
        slots = get_slots(intent_request)

        validation_result = validate_dining_slots(
            location, cuisine, number_of_people, dining_date, dining_time, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {
        }
        return delegate(output_session_attributes, get_slots(intent_request))
    
    sendSQS(slots)
    
    return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': ' You’re all set. My suggestions will be sent to you shortly on email!'})



def dispatch(intent_request):

    logger.debug('dispatch userId={}, intentName={}'.format(
        intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')



""" Main handler """


def lambda_handler(event, context):
    print("LF1 -Invoked: ", event)
    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    return dispatch(event)