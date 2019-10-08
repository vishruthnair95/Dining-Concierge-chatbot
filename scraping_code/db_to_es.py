import boto3
import requests
from elasticsearch import Elasticsearch


dynamodb = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="http://dynamodb.us-east-2.amazonaws.com")


table = dynamodb.Table('restaurant_test')

data = table.scan()
doc2 =dict()

es = Elasticsearch(['https://search-yelprestaurant1-xpkmlkk3p5qgkjo5hujfmrvzha.us-east-2.es.amazonaws.com'])
ct = 0
for doc in data['Items']:
    ct+=1
    for key in doc:
        if key in ['name', 'id','categories']:
            doc2[key] = doc[key]
    res = es.index(index="restaurants", doc_type='Restaurant', id=doc2['id'], body=doc2)
    print(res['result'], ct)
  
