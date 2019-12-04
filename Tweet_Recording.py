# Brandon Fisher
# Senior Project - Fall 2019

# Python application to record tweets mentioning specific companies

import requests
import json
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
from datetime import datetime, timedelta
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
    items = {}
    for search in db_cursor.fetchall():
        if search[0] not in items.keys():
            items.update({search[0]: [search[1]]})
        else:
            items[search[0]].append(search[1])
    return items

def GetOldestTimestamp(db_cursor, company_id):
    query = """SELECT post_time FROM tweet WHERE company_id = {0} ORDER BY post_time asc FETCH FIRST 1 ROWS ONLY;""".format(company_id)
    ExecuteStatement(db_cursor, query)

    res = db_cursor.fetchone()
    return datetime(res[0])

def GetLatestTimestamp(db_cursor, company_id):
    query = """SELECT post_time FROM tweet WHERE company_id = {0} ORDER BY post_time desc FETCH FIRST 1 ROWS ONLY;""".format(company_id)
    ExecuteStatement(db_cursor, query)

    res = db_cursor.fetchone()
    return res[0]

def GetMinTweetIdGreaterThanTime(db_cursor, company_id, tweet_id_cutoff):
    query = "SELECT twitter_tweet_id FROM tweet WHERE company_id = {0} AND twitter_tweet_id > '{1}' ORDER BY twitter_tweet_id asc FETCH FIRST 1 ROWS ONLY;".format(company_id, tweet_id_cutoff)

    ExecuteStatement(db_cursor, query)
    return db_cursor.fetchone()[0]

def UpdateMinMaxTweetId(db_cursor, id_min_max, idents):
    base_query = "SELECT twitter_tweet_id FROM tweet WHERE company_id = "
    for ident in idents:
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
    return id_min_max

def GetCompanyMinMaxTweetId(db_cursor):
    ExecuteStatement(db_cursor, "SELECT company_id FROM company;")
    ids = db_cursor.fetchall()
    id_min_max = {ident[0]: {'min': '0', 'max': '0'} for ident in ids}
    id_min_max = UpdateMinMaxTweetId(db_cursor, id_min_max, [ident[0] for ident in ids])
    return id_min_max

def StoreCompanyTweets(db_cursor, company_id, tweets):
    insert_statement = u"""INSERT INTO tweet (company_id, twitter_tweet_id, full_text, post_time) VALUES """

    sql_params = []

    for tweet in tweets:
        twitter_tweet_id = tweet['id_str']
        full_text = tweet['full_text']
        post_time = datetime.strptime(str(tweet['created_at']).replace('+0000', ''), '%a %b %d %H:%M:%S %Y')

        insert_statement += "(%s, %s, %s, %s), "
        sql_params.append(company_id)
        sql_params.append(twitter_tweet_id)
        sql_params.append(full_text)
        sql_params.append(str(post_time))

    insert_statement = insert_statement[:-2] + """;"""
    db_cursor.execute(insert_statement, sql_params)

def CreateSearch(api, params):
    gotResponse = False
    count = 0
    while not gotResponse:
        try:
            res = api.request('search/tweets', params).json()
            if 'statuses' in res.keys():
                return api, res['statuses']
            else:
                return api, []
        except:
            print("Reconnecting to twitter")
            time.sleep(5)
            api = InitializeTwitterApi()
            count += 1
            if count > 10:
                print('Failed to reconnect to Twitter, returning [].  No results for this search:\n', params, '\n\n')
                return api, []


def GetAndStorePastTweets(api, company_id, id_min_max, search_terms):
    for term in search_terms:
        oldest_tweets_found = False
        while not oldest_tweets_found:
            params = {}
            params.update({'lang': 'en'})
            params.update({'tweet_mode': 'extended'})
            params.update({'count': '100'})
            params.update({'result_type': 'recent'})
            params.update({'q': term})
            if(id_min_max[company_id]['min'] != '0'):
                params.update({'max_id': id_min_max[company_id]['min']})

            start_time = datetime.now()
            api, tweet_batch = CreateSearch(api, params)

            if (len(tweet_batch) <= 1):
                oldest_tweets_found = True
            else:
                db_conn = ConnectToDatabase()
                StoreCompanyTweets(db_conn.cursor(), company_id, tweet_batch)
                CommitAndClose(db_conn.cursor(), db_conn)

                db_conn = ConnectToDatabase()
                id_min_max = UpdateMinMaxTweetId(db_conn.cursor(), id_min_max, [company_id])
                CommitAndClose(db_conn.cursor(), db_conn)

            duration = datetime.now() - start_time

            if duration.total_seconds() < 5:
                time.sleep(round(5 - duration.total_seconds(), 2) + .01)
    return api, id_min_max

def GetAndStoreLatestTweets(api, company_id, id_min_max, search_terms):
    db_conn = ConnectToDatabase()
    max_current = UpdateMinMaxTweetId(db_conn.cursor(), id_min_max, [company_id])[company_id]['max']
    CommitAndClose(db_conn.cursor(), db_conn)

    min_new = '0'

    term_done = {term: False for term in search_terms}

    while False in term_done.values():

        new_tweets_added = False
        for term in term_done.keys():
            if not term_done[term]:
                params = {}
                params.update({'lang': 'en'})
                params.update({'tweet_mode': 'extended'})
                params.update({'count': '100'})
                params.update({'q': term})
                params.update({'since_id': max_current})
                if(min_new != '0'):
                    params.update({'max_id': min_new})

                start_time = datetime.now()
                api, tweet_batch = CreateSearch(api, params)

                if(len(tweet_batch) <= 2):
                    term_done[term] = True
                else:
                    db_conn = ConnectToDatabase()
                    StoreCompanyTweets(db_conn.cursor(), company_id, tweet_batch)
                    new_tweets_added = True
                    CommitAndClose(db_conn.cursor(), db_conn)
                
                duration = datetime.now() - start_time
                if duration.total_seconds() < 5:
                    time.sleep(round(5 - duration.total_seconds(), 2) + .01)

        if new_tweets_added:        
            db_conn = ConnectToDatabase()
            min_new = GetMinTweetIdGreaterThanTime(db_conn.cursor(), company_id, max_current)
            CommitAndClose(db_conn.cursor(), db_conn)
    return api, id_min_max


api = InitializeTwitterApi()

db_conn = ConnectToDatabase()
db_cursor = db_conn.cursor()

ident_min_max = GetCompanyMinMaxTweetId(db_cursor)

terms = GetSearchTerms(db_cursor)

CommitAndClose(db_cursor, db_conn)

for company in terms.keys():
    api, ident_min_max = GetAndStorePastTweets(api, company, ident_min_max, terms[company])

while True:
    db_conn = ConnectToDatabase()
    db_cursor = db_conn.cursor()
    terms = GetSearchTerms(db_cursor)
    CommitAndClose(db_cursor, db_conn)
    api = InitializeTwitterApi()
    for company in terms.keys():
        api, ident_min_max = GetAndStoreLatestTweets(api, company, ident_min_max, terms[company])

    time.sleep(60)

