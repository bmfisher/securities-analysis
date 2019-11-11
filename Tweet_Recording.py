# Brandon Fisher
# Senior Project - Fall 2019

# Python application to record tweets mentioning specific companies

import requests
import json
import psycopg2
from datetime import datetime
from pytz import timezone
import time
import TwitterAPI

CONSUMER_KEY = 'Omh2yjIJ5wQHAO9svFfLzymOx'
CONSUMER_SECRET = 'UAMhVFK2sSdez92DbYAw1TPcWmxUFRhpDJbY3WdN5H5GxxqIJv'
ACCESS_TOKEN = '1162080468289744896-rPDvTmJ0BqDYXFhcmUz9E013wKsV8P'
ACCESS_SECRET = 'vs5Xye75fwIs4lLqlaexRAlsGsONFclB6h48OzJ1gDSGl'


def InitializeTwitterApi():
    return TwitterAPI.TwitterAPI(consumer_key=CONSUMER_KEY,
                  consumer_secret=CONSUMER_SECRET,
                  access_token_key=ACCESS_TOKEN,
                  access_token_secret=ACCESS_SECRET)

def ConnectToDatabase():
    connection_string = "host=ls-09e48ef281d3784d651efb2f69c508d20bec3da8.c8o3a3nfv7m4.us-east-2.rds.amazonaws.com"
    connection_string += " user=dbmasteruser"
    connection_string += " dbname=postgres"
    connection_string += " password=XIC[m#7-*foCL~GQtREEN~49ZLsg}>*$"
    
    db_connection = psycopg2.connect(connection_string)
    return db_connection
    
def CommitAndClose(db_cursor, db_connection):
    db_connection.commit()
    db_cursor.close()
    db_connection.close()

def ExecuteStatement(db_cursor, statement):
    db_cursor.execute(statement)

def IsWeekday(date):
    return date.weekday() >= 0 and date.weekday() <= 4

def GetSearchTerms(db_cursor):
    ExecuteStatement(db_cursor, "SELECT company_id, search_text FROM search;")
    return {search[0]:search[1] for search in db_cursor.fetchall()}

def UpdateMinMaxTweetId(db_cursor, id_min_max):
    base_query = "SELECT twitter_tweet_id FROM tweet WHERE company_id = "
    for ident in id_min_max.keys():
        max_query = base_query + str(ident) + " ORDER BY post_time desc FETCH FIRST 1 ROWS ONLY;"
        min_query = base_query + str(ident) + " ORDER BY post_time asc FETCH FIRST 1 ROWS ONLY;"
        ExecuteStatement(db_cursor, max_query)
        max_tweet_id = db_cursor.fetchone()
        ExecuteStatement(db_cursor, min_query)
        min_tweet_id = db_cursor.fetchone()
        if(min_tweet_id):
            id_min_max[ident]['min'] = min_tweet_id[0]
        if(max_tweet_id):
            id_min_max[ident]['max'] = max_tweet_id[0]

def GetCompanyMinMaxTweetId(db_cursor):
    ExecuteStatement(db_cursor, "SELECT company_id FROM company;")
    ids = db_cursor.fetchall()
    id_min_max = {ident[0]: {'min': '0', 'max': '0'} for ident in ids}
    UpdateMinMaxTweetId(db_cursor, id_min_max)
    return id_min_max

def StoreCompanyTweets(db_cursor, company_id, tweets):
    insert_statement = "INSERT INTO tweet (company_id, twitter_tweet_id, full_text, post_time) VALUES "

    for tweet in tweets:
        twitter_tweet_id = str(tweet['id_str'])
        full_text = str(tweet['full_text']).replace("'", "''")
        post_time = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')

        insert_statement += "({0}, '{1}', '{2}', '{3}'), ".format(company_id, twitter_tweet_id, full_text, post_time)

    insert_statement = insert_statement[:-2] + ";"
    print(insert_statement)
    ExecuteStatement(db_cursor, insert_statement)

def CreateSearch(api, params):
    return api.request('search/tweets', params).json()['statuses']

def PrintResults(res):
    for tweet in res:
        print("Date: ", datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y'), "\n\t Text: ", tweet['full_text'])


api = InitializeTwitterApi()

res = CreateSearch(api, {'q': '@jpmorgan', 'count': '10', 'tweet_mode': 'extended'})

db_conn = ConnectToDatabase()
db_cursor = db_conn.cursor()

