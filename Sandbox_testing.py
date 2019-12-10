# data process testing

import requests
import json
import psycopg2
from datetime import datetime
from pytz import timezone
import time

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

def GetDbCompany():
    db_conn = ConnectToDatabase()
    db_cursor = db_conn.cursor()
    query = "SELECT company_id, full_name, ticker FROM company;"
    db_cursor.execute(query)
    company_info = {company[0]: (company[1], company[2]) for company in db_cursor.fetchall()}
    CommitAndClose(db_cursor, db_conn)
    return company_info

def GetDbPrice():
    db_conn = ConnectToDatabase()
    db_cursor = db_conn.cursor()
    query = "SELECT company_id, quote_time, price FROM price;"
    db_cursor.execute(query)
    price_raw_data = db_cursor.fetchall()
    CommitAndClose(db_cursor, db_conn)
    price_info = {}
    for price in price_raw_data:
        if price[0] in price_info.keys():
            price_info[price[0]].append((price[1], price[2]))
        else:
            price_info.update({price[0]: [(price[1], price[2])]})
    return price_info

db_company = GetDbCompany()
db_price = GetDbPrice()