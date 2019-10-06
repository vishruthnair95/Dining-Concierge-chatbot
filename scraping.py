from __future__ import print_function
import json
import requests
import boto3
from decimal import*
import datetime

def cat(categories):
	a=[]
	for i in categories:
		a.append(i['title'])
	if len(a):
		return a
	else:
		return ' '
	


def exist(a):	
	if (a):
		return a
	else:
		return " "




if __name__ == "__main__":
			
	client_id = 'Hm78lXMQaCTeqCkBN5lhPQ'
	api_key = 'FCm-aMHgLy05AGcbrJ3D04FG3iaG9qcABwbyEKoTy-GI-UshdtYSkvtHWVaTxWDU93IMjrGTcpSPpZAY-0Q9TOv_tJP1yJfSpz7k0wadsQZbb8DWOe-cRKIRHBSVXXYx'
	print(datetime.datetime.utcnow())
	total_restaurants ={}
	ct=0

	term = ['Mexican', 'American', 'French', 'Indian', 'Chinese', 'Japanese', 'Thai', 'Korean', 'Spanish', 'Vegan', 'Italian']
	location = 'NY'
	SEARCH_LIMIT = 50
	offset = 0
	url = 'https://api.yelp.com/v3/businesses/search'

	headers = {
	        'Authorization': 'Bearer {}'.format(api_key),
	    }

	url_params = {
		                'term': term[0].replace(' ', '+'),
		                'location': location.replace(' ', '+'),
		                'limit': SEARCH_LIMIT,
		                'offset': offset
		            }
	response = requests.get(url, headers=headers, params=url_params)
	json_data = json.loads(response.text)

	for i in json_data["businesses"]:
		total_restaurants[i["id"]] =i
		
	
	for type in term:	
		for i in range(0,20):
			offset = 50*i  	
			url_params = {
			                'term': type.replace(' ', '+'),
			                'location': location.replace(' ', '+'),
			                'limit': SEARCH_LIMIT,
			                'offset': offset
			            }
			response = requests.get(url, headers=headers, params=url_params)
			json_data = json.loads(response.text)
			for j in json_data["businesses"]:
					total_restaurants[j["id"]] = j
		print(type, "dne", len(total_restaurants))
				
				

	#print(json.dumps(json_data["businesses"], indent=4, sort_keys=True))
	print("as",len(total_restaurants))




	dynamodb = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="http://dynamodb.us-east-2.amazonaws.com")

	table = dynamodb.Table('restaurant_test')
	ct=0
	for i in total_restaurants:
			print(total_restaurants[i]['location'])
			table.put_item(
				Item={
					"id": exist(total_restaurants[i]['id']),

					# string, the business's name
					"name": exist(total_restaurants[i]['name']),
	
					# # string, the full address of the business
					"address": exist(total_restaurants[i]['location']['address1']),

					# # string, the city
					"city": exist(total_restaurants[i]['location']['city']),

					# string, 2 character state code, if applicable
					"state": exist(total_restaurants[i]['location']['state']) ,

					# string, the postal code
					"zip_code": exist(total_restaurants[i]['location']['zip_code']),
	

					# float, star rating, rounded to half-stars
					"rating": exist(Decimal(total_restaurants[i]['rating'])),

					# integer, 0 or 1 for closed or open, respectively
					"is_closed": exist(total_restaurants[i]['is_closed']),

					#an array of strings of business categories
					"categories": cat(total_restaurants[i]['categories']),

					"Timestamp": str(datetime.datetime.utcnow())
					})
			print("Table status:", table.table_status)
