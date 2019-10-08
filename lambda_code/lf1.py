import json
import boto3
from botocore.exceptions import ClientError
    
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": fulfillment_state,
            "message": message
        }
    }
     
    return response
    
def build_response_message(message_content):
    return {'contentType':'PlainText', 'content': message_content}
    
def send_sqs_message(sqs_queue_url, msg_body):
    sqs_client = boto3.client('sqs')
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=msg_body)
    except ClientError as e:
        logging.error(e)
        return None
    return msg
    
def lambda_handler(event, context):
    intent_name = event['currentIntent']['name']
    session_attributes = event['sessionAttributes']
    print('received request: {}'.format(event))
    print('intent name: {}'.format(intent_name))
    
    #Start by validating all input
    #validate location
    location = event['currentIntent']['slots']['Location']
    if(location.lower() != "manhattan"):
        lex_response = "We currently only do reservations in Manhattan. Would you like to try reserving again in Manhattan?"
        return close(session_attributes, 'Fulfilled', build_response_message(lex_response))
    
    #validate cuisine
    cuisine = event['currentIntent']['slots']['Cuisine']
    cuisine_list = ['mexican', 'italian', 'french', 'american', 'indian', 'chinese', 'korean', 'spanish', 'thai', 'japanese', 'vegan']
    
    if(cuisine.lower() not in cuisine_list):
        lex_response = "We currently don't do recommendations for {} restuarants. Would you like to try another reservation?".format(cuisine)
        return close(session_attributes, 'Fulfilled', build_response_message(lex_response))
    
    #validate number of people
    num_people = event['currentIntent']['slots']['NumberPeople']
    
    if(int(num_people) < 1 or int(num_people) > 20):
        lex_response = "We only do reservations for between 1-20 people. Would you like to try another reservation?"
        return close(session_attributes, 'Fulfilled', build_response_message(lex_response))
    
    #validate phone number 
    phone_number = event['currentIntent']['slots']['PhoneNumber']
    d_time = event['currentIntent']['slots']['DiningTime']
    
    if(len(str(phone_number)) != 10):
        lex_response = "Your number seems invalid. We only take 10 digit American phone numbers. Would you like to try another reservation?"
        return close(session_attributes, 'Fulfilled', build_response_message(lex_response))
    
    #write to SQS
    r = {'location': location, 'cuisine': cuisine, 'num_people': num_people, 'phone_number': phone_number, 'dining_time': d_time} 
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/875654307671/UserRestaurantInformation'
    message = json.dumps(r)
    send_sqs_message(sqs_url, message)
    
    
    #send proper response
    lex_response = "We've got it from here: You will be receiving a text message at {} for {} restaurants in {} for {} people at {}. Would you like to make another reservation?".format(phone_number, cuisine, location, num_people, d_time)
    return close(session_attributes, 'Fulfilled', build_response_message(lex_response))

