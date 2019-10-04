# Brandon Fisher
# Senior Project - Fall 2019

# Python application to record intraday stock prices

import requests
import json
import psycopg2
from datetime import datetime
from pytz import timezone
import time

def CommitAndClose(db_cursor, db_connection):
    db_connection.commit()
    db_cursor.close()
    db_connection.close()

def GetMostRecentStoredPriceDatetime():
    db_connection = ConnectToDatabase()
    db_cursor = db_connection.cursor()
    time_query = "SELECT DISTINCT quote_time FROM price ORDER BY quote_time FETCH FIRST 1 ROWS ONLY;"
    db_cursor.execute(time_query)
    time = db_cursor.fetchone()
    
    CommitAndClose(db_cursor, db_connection)
    if time != None:
        return time[0]
    else:
        return datetime(2010,1,1)

def StoredTodaysPrices(current_time):
    last_stored_time = GetMostRecentStoredPriceDatetime()
    return last_stored_time.year == current_time.year and last_stored_time.month == current_time.month and last_stored_time.day == current_time.day

def IsWeekday(date):
    return date.weekday() >= 0 and date.weekday() <= 4

def ConnectToDatabase():
    connection_string = "host=ls-09e48ef281d3784d651efb2f69c508d20bec3da8.c8o3a3nfv7m4.us-east-2.rds.amazonaws.com"
    connection_string += " user=dbmasteruser"
    connection_string += " dbname=postgres"
    connection_string += " password=XIC[m#7-*foCL~GQtREEN~49ZLsg}>*$"
    
    db_connection = psycopg2.connect(connection_string)
    return db_connection

def GetTickers(db_cursor):
    queryString = "SELECT company_id, ticker FROM company;"
    db_cursor.execute(queryString)
    return {company[1]:company[0] for company in db_cursor.fetchall()}

def GetIntradayPrices(tickers):
    # Establish constants for IEX API
    base_url = "https://cloud.iexapis.com/"
    api_version = "v1/"
    validation_token = "?token=pk_2939d36e376a45ca850fd1b162764e1e"

    company_prices = {}

    for ticker in tickers.keys():
        call_url = base_url + api_version + "stock/" + ticker + "/intraday-prices" + validation_token
        day_prices = requests.get(call_url).json()
        company_prices.update(
            {ticker : [
                {'date': price_time['date'], 'minute': price_time['minute'], 'price': price_time['close']}
                for price_time in day_prices
            ]})

    return company_prices

def CreateInsertStatement(company_prices, tickers):
    insert_statement = "INSERT INTO price (company_id, quote_time, price) VALUES "

    for company in company_prices.keys():
        for price_time in company_prices[company]:
            if price_time['price'] != None:
                company_id = tickers[company]
                price = price_time['price']

                year, month, day = [int(item) for item in price_time['date'].split('-')]
                hour, minute = [int(item) for item in price_time['minute'].split(':')]
                quote_time = datetime(year,month,day,hour,minute)

                insert_statement += "({0}, '{1}', {2}), ".format(company_id, quote_time, price)

    insert_statement = insert_statement[:-2]
    return insert_statement + ";"

def ExecuteStatement(db_cursor, statement):
    db_cursor.execute(statement)

# Run the program

def ProcessTodaysPrices():
    db_connection = ConnectToDatabase()
    db_cursor = db_connection.cursor()
    tickers = GetTickers(db_cursor)
    # company_prices in the form 
    # {'ticker': [{'date': 'yyyy-mm-dd', 'minute': 'hh:mm', 'price': price}, {}, {} etc.]}
    company_prices = GetIntradayPrices(tickers)
    insert_statement = CreateInsertStatement(company_prices, tickers)
    ExecuteStatement(db_cursor, insert_statement)
    CommitAndClose(db_cursor, db_connection)

while(True):
    current_time = datetime.now(timezone('EST'))
    print(current_time)
    if IsWeekday(current_time) and current_time.hour >= 17 and current_time.hour <= 22 and not StoredTodaysPrices(current_time):
        print("Processing Prices")
        ProcessTodaysPrices()
    
    time.sleep(3600)