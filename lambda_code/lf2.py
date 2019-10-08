import json
import time
import boto3
from botocore.exceptions import ClientError
from botocore.vendored import requests
#from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
import random

def get_sqs_message(sqs_queue_url):
    sqs_client = boto3.client('sqs')
    
    try:
        #get message
        msg = sqs_client.receive_message(QueueUrl=sqs_queue_url,
                                      MaxNumberOfMessages=1)
        
        print(msg)                              
        #delete message from queue
        
        if ('Messages' in msg) and (len(msg['Messages']) == 1):
            receipt_handle = msg['Messages'][0]['ReceiptHandle']
            delete_msg = sqs_client.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handle)
    except ClientError as e:
        print(e)
        return None
    return msg
    
def send_sns_message(phone_number, txt_message):
    sns_client = boto3.client("sns")
    
    try:
        text_number = "+1{}".format(phone_number)
        msg = sns_client.publish(
                    PhoneNumber=text_number,
                    Message=txt_message
                )
    except ClientError as e:
        print(e)
        return None
    return msg
    
def query_es(query, fields):
    region = '' 
    service = 'es'
    #credentials = boto3.Session().get_credentials()
    #awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    host = 'https://search-yelprestaurant1-xpkmlkk3p5qgkjo5hujfmrvzha.us-east-2.es.amazonaws.com'
    index = 'restaurants'
    url = host + '/' + index + '/_search'
    
    query = {
        "size": 250,
        "query": query
    }

    # ES 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }

    # Make the signed HTTP request
    #r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    # print(query)
    r = requests.get(url, headers=headers, data=json.dumps(query))
    
    return r.json()
    

    

def lambda_handler(event, context):
    print("event data: {}".format(event) )
    
    #poll from sqs
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/875654307671/UserRestaurantInformation'
    poll_msg = get_sqs_message(sqs_url)
    # print(poll_msg)
    
    #search for appropriate restaurant
    if ('Messages' in poll_msg) and len(poll_msg['Messages']) == 1:
        poll_dict = json.loads(poll_msg['Messages'][0]['Body'])
        print(poll_dict)
        dining_time = poll_dict['dining_time']
        cuisine = poll_dict['cuisine']
        num_people = poll_dict['num_people']
        phone_number = poll_dict['phone_number']
        location = poll_dict['location']
        
        #search elasticsearch and return id of a restaurant in that category
        formatted_query = {"match": {"categories": cuisine}}
        print(formatted_query)
        resp = query_es(formatted_query,cuisine)
        
        msg_text = 'Hi, this is Recommender Bot. The reservation is for a {} restuarant for {} people at {}. We have the following options:\n'.format(cuisine, num_people, dining_time)
        msg_text_choices = []
        for choice in range(3):
            rest_data = random.choice(resp["hits"]["hits"])
            rest_id = rest_data['_source']['id']
            print(rest_id)
            
            #search dynamodb for corresponding restaurant and get back 
            dynamodb = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="http://dynamodb.us-east-2.amazonaws.com")
            table = dynamodb.Table('restaurant_test')
            response = table.query(
                KeyConditionExpression=Key("id").eq(rest_id))
            
            print(response["Items"])
            rest_data = response["Items"][0]
            r_name = rest_data["name"]
            r_rating = rest_data["rating"]
            r_price = rest_data["price"]
            r_phone = rest_data["phone"]
            r_address = rest_data["address"]
            r_num_rating = rest_data["review_count"]
        
            #format text 
            msg_text_choices.append(str(choice+1) +') Restaurant called {}. The address is {} in {}. It has a {} star rating (on {} ratings) and has a price rating of {}.\n '.format(r_name, r_address, location, r_rating, r_num_rating, r_price))
            print(msg_text_choices[choice])        
        
        msg_text= msg_text + msg_text_choices[0]+msg_text_choices[1] + msg_text_choices[2]
        
        #send text message
        return_val = send_sns_message(phone_number, msg_text)
        
        return {
            'statusCode': 200,
            'body': 'Text Message was sent with messageid: {}'.format(return_val)
        }
    
    return {
        'statusCode': 200,
        'body': 'No new messages in queue'
    }