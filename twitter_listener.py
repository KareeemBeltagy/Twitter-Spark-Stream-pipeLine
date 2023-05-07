# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For dealing with json responses we receive from the API
import json
# For displaying the data after
import pandas as pd
# For saving the response data in CSV format
import csv
# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
#To add wait time between requests
import time
#To open up a port to forward tweets
import socket 


os.environ['TOKEN'] =  "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)
 
def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "messi OR cristiano lang:en"
start_time = "2023-05-02T17:00:00.000Z"
end_time = "2023-05-03T22:00:00.000Z"
max_results = 100

url = create_url(keyword, start_time,end_time, max_results)
# json_response = connect_to_endpoint(url[0], headers, url[1])
next_token = None

s = socket.socket()
host = "127.0.0.1"
port = 7777
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
print("Received request from: " + str(address)," connection created.")
print("==============================================================")


c=1            
while True:
    json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
    data = json_response["data"]
    includes = json_response.get("includes", [])
    
    for data in data:
        tweet = {}
        tweet['created_at'] = data['created_at']
        tweet['text'] = data['text']
        tweet['tweet_id'] = data['id']
        for key, value in data['public_metrics'].items():
            tweet[key] = value
        json_data = json.dumps(tweet)
        print("Sending:", (json_data).encode('utf-8'))
        clientsocket.send((json_data + '\n').encode('utf-8'))
        print("=============================================================="+str(c))
        c += 1
        time.sleep(2)
    if 'next_token' in json_response['meta']:
        next_token = json_response['meta']['next_token']
    else:
        break
    
clientsocket.close()