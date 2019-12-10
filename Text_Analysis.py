# Brandon Fisher
# Senior Project - Fall 2019

# Python application to analyze tweet text

import json
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
from datetime import datetime, timedelta
from pytz import timezone
import time
from textblob import TextBlob

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

def GetNextBatchOfTweets(db_cursor, batch_size):
    query = "SELECT tweet_id, full_text FROM tweet WHERE sentiment IS NULL ORDER BY tweet_id ASC FETCH FIRST " + str(batch_size) + " ROWS ONLY"
    ExecuteStatement(db_cursor, query)
    results = db_cursor.fetchall()
    return {tweet[0]:tweet[1] for tweet in results}

def UpdateSentimentInDatabase(db_cursor, tweet_id_sentiment):
    ids = tweet_id_sentiment.keys()
    max_id, min_id = max(ids), min(ids)

    sql_params = []
    update_statement = """UPDATE tweet SET sentiment = CASE """
    for ident in ids:
        update_statement += "WHEN tweet_id = %s THEN %s"
        sql_params.append(ident)
        sql_params.append(tweet_id_sentiment[ident])
    update_statement += """END WHERE tweet_id BETWEEN %s AND %s;"""
    sql_params.append(min_id)
    sql_params.append(max_id)
    db_cursor.execute(update_statement, sql_params)

def IsCaughtUp(batch_size):
    db_connection = ConnectToDatabase()
    db_cursor = db_connection.cursor()
    query = "SELECT count(*) FROM tweet WHERE sentiment IS NULL;"
    db_cursor.execute(query)
    remaining = db_cursor.fetchall()
    CommitAndClose(db_cursor, db_connection)
    return remaining[0][0] < batch_size * 2


def AnalyzeTweets(tweet_id_text):
    blob = None
    tweet_id_sentiment = {}
    for ident in tweet_id_text.keys():
        blob = TextBlob(tweet_id_text[ident])
        tweet_id_sentiment.update({ident:round(blob.sentiment.polarity, 6)})
    return tweet_id_sentiment

start_time = datetime.now(timezone('EST'))
print(start_time)
done = False
while not done:
    db_connection = ConnectToDatabase()
    db_cursor = db_connection.cursor()
    batch_size = 100
    tweet_id_text = GetNextBatchOfTweets(db_connection.cursor(), batch_size)
    tweet_id_sentiment = AnalyzeTweets(tweet_id_text)
    UpdateSentimentInDatabase(db_cursor, tweet_id_sentiment)
    CommitAndClose(db_cursor, db_connection)

    if datetime.now(timezone('EST')) - start_time > timedelta(minutes=60) or IsCaughtUp(batch_size):
        done = True

end_time = datetime.now(timezone('EST'))

print(start_time, end_time)
print(end_time - start_time)
    
# tweet_id_text = GetNextBatchOfTweets(db_cursor, 15)
# tweet_id_sentiment = AnalyzeTweets(tweet_id_text)
# UpdateSentimentInDatabase(db_cursor, tweet_id_sentiment)
# CommitAndClose(db_cursor, db_connection)