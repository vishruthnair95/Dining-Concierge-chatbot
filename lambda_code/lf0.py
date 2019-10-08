import json
import time
import boto3

def lambda_handler(event, context):
    print("event data: {}".format(event) )
    client = boto3.client('lex-runtime')
    
    user_input = event['data']
    count = event['count']
    
    if count == 1:
        user_input = "Hi my name is {}".format(user_input)
    
    #feed input to Lex and get response
    response = client.post_text(
            botName='RestaurantRecBot',
            botAlias='versionone',
            userId='platform_tester',
            inputText= user_input
        )
    print(response)
    
    #feed back output to chatbot
    
    return {
        'statusCode': 200,
        'body': response['message']
    }
    