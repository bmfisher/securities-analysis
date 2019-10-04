import requests
import json
import psycopg2
import datetime

base = "https://cloud.iexapis.com/"
version = "v1/"
token = "?token=pk_2939d36e376a45ca850fd1b162764e1e"

connstr = "host=ls-09e48ef281d3784d651efb2f69c508d20bec3da8.c8o3a3nfv7m4.us-east-2.rds.amazonaws.com"
connstr += " user=dbmasteruser"
connstr += " dbname=postgres"
connstr += " password=XIC[m#7-*foCL~GQtREEN~49ZLsg}>*$"

conn = psycopg2.connect(connstr)
cur = conn.cursor()

symbols = [
    'MMM', 
    'AXP', 
    'AAPL',
    'BA',
    'CAT',
    'CVX',
    'CSCO',
    'KO',
    'DOW',
    'XOM',
    'GS',
    'HD',
    'IBM',
    'INTC',
    'JNJ',
    'JPM',
    'MCD',
    'MRK',
    'MSFT',
    'NKE',
    'PFE',
    'PG',
    'TRV',
    'UNH',
    'UTX',
    'VZ',
    'V',
    'WMT',
    'WBA',
    'DIS']

# for ticker in symbols:
#     url = base + version + "stock/" + ticker + "/intraday-prices" + token
#     result = requests.get(url).json()
#     print("Symbol: ", result["symbol"])
#     print("Company: ", result["delayedPrice"])
#     print("Exchange: ", result["delayedPriceTime"])
#     print("\n\n")
    # url = base + version + "stock/" + k + "/quote"  + token
    # res = requests.get(url).json()

url = base + version + "stock/VZ/intraday-prices" + token
result = requests.get(url).json()



print(result)