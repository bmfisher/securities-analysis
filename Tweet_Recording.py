# Brandon Fisher
# Senior Project - Fall 2019

# Python application to record tweets mentioning specific companies

import requests
import json
import psycopg2
from datetime import datetime
from pytz import timezone
import time
import oauth2
import base64
import urllib.parse
import TwitterAPI

CONSUMER_KEY = 'Omh2yjIJ5wQHAO9svFfLzymOx'
CONSUMER_SECRET = 'UAMhVFK2sSdez92DbYAw1TPcWmxUFRhpDJbY3WdN5H5GxxqIJv'
OAUTH2_TOKEN = 'https://api.twitter.com/oauth2/token'
ACCESS_TOKEN = '1162080468289744896-rPDvTmJ0BqDYXFhcmUz9E013wKsV8P'
ACCESS_SECRET = 'vs5Xye75fwIs4lLqlaexRAlsGsONFclB6h48OzJ1gDSGl'

def get_bearer_token(consumer_key, consumer_secret):
    # enconde consumer key
    consumer_key = urllib.parse.quote(consumer_key)
    # encode consumer secret
    consumer_secret = urllib.parse.quote(consumer_secret)
    # create bearer token
    bearer_token = consumer_key + ':' + consumer_secret
    # base64 encode the token
    base64_encoded_bearer_token = base64.b64encode(bearer_token.encode('utf-8'))
    # set headers
    headers = {
        "Authorization": "Basic " + base64_encoded_bearer_token.decode('utf-8') + "",
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Content-Length": "29"}

    response = requests.post(OAUTH2_TOKEN, headers=headers, data={'grant_type': 'client_credentials'})
    to_json = response.json()
    return to_json['access_token']

api = TwitterAPI.TwitterAPI(consumer_key=CONSUMER_KEY,
                  consumer_secret=CONSUMER_SECRET,
                  access_token_key=ACCESS_TOKEN,
                  access_token_secret=ACCESS_SECRET)


def createSearch(params):
    return api.request('search/tweets', params).json()['statuses']

# def build_url(search="", maxResults='100', fromDate='201910100930', toDate='201910101600'):
#     base = 'https://api.twitter.com/1.1/search/'
#     # product = '30day/'
#     label = 'tweets.json'
#     querystring = '?query=' + search + ' lang:en' + '&maxResults=' + maxResults
#     querystring += '&fromDate=' + fromDate + '&toDate=' + toDate
#     return base+label+querystring

# test_token = get_bearer_token(CONSUMER_KEY, CONSUMER_SECRET)

# def getResult(url=build_url(search='@jpmorgan')):
#     return requests.get(url, headers={'Authorization': 'Bearer '+test_token}).json()

def printResults(res):
    for tweet in res:
        print("Date: ", datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y'), "\n\t Text: ", tweet['full_text'])

# res = getResult(build_url(search='@jpmorgan'))
# tweets = res['results']
res = createSearch({'q': '@jpmorgan', 'count': '10', 'tweet_mode': 'extended'})


#print(testResult.json())
#datetime.strptime(times[0], "%a %b %d %H:%M:%S %z %Y")

#Must account for truncation if tweet['truncated'] is true - look for tweet['extended_tweet']['full_text']