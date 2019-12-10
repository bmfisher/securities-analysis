# Brandon Fisher
# CS 598 Fall 2019 Senior Project
#
# Dash application for data visualization

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

def GetDbTweet():
    db_conn = ConnectToDatabase()
    db_cursor = db_conn.cursor()
    query = "SELECT company_id, post_time, sentiment FROM tweet WHERE sentiment IS NOT NULL;"
    db_cursor.execute(query)
    tweet_raw_data = db_cursor.fetchall()
    CommitAndClose(db_cursor, db_conn)
    tweet_info = {}
    for tweet in tweet_raw_data:
        if tweet[0] in tweet_info.keys():
            tweet_info[tweet[0]].append((tweet[1], tweet[2]))
        else:
            tweet_info.update({tweet[0]: [(tweet[1], tweet[2])]})
    return tweet_info

def KNeighborAverage(time_tweet_list, k):
    averaged_list = []
    i = 0
    while i < len(time_tweet_list) - 2*k - 1:
        averaged_list.append(
            (
                time_tweet_list[i+k][0], 
                sum([ts[1] for ts in time_tweet_list[i:i+2*k]]) / len(time_tweet_list[i:i+2*k])
            ))
        i += k
    return averaged_list

db_company = GetDbCompany()
db_price = GetDbPrice()
db_tweet = GetDbTweet()
for time_tweet_list in db_tweet.values():
    time_tweet_list.sort()

app = dash.Dash()

app.layout = html.Div(children=[
    html.H1(children='Sentiment & Securities Analysis'),

    html.Div(children='''
        Brandon Fisher - CS Senior Project, Kansas State University 2019, built with Dash
    '''),

    dcc.Dropdown(
        id='ticker-select',
        options=[
            {'label': db_company[company_id][1] + ' - ' + db_company[company_id][0], 'value': company_id} for company_id in db_company.keys()
        ], 
        value=11
    ),

    dcc.Dropdown(
        id='sentiment-neighbor-average',
        options=[
            {'label': 'None', 'value': 0},
            {'label': '5', 'value': 5},
            {'label': '10', 'value': 10},
            {'label': '25', 'value': 25}
        ],
        value=5
    ),

    dcc.Graph(id='stocks-vs-sentiment')
])

@app.callback(
    Output('stocks-vs-sentiment', 'figure'),
    [Input('ticker-select', 'value'),
    Input('sentiment-neighbor-average', 'value')]
)
def update_graph(company_key, k_average):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=[time_price[0] for time_price in db_price[company_key]], 
            y=[time_price[1] for time_price in db_price[company_key]], 
            name='stock-data'
        ),
        secondary_y=False,
    )

    if k_average == 0:
        averaged_sentiments = db_tweet[company_key]
    else:
        averaged_sentiments = KNeighborAverage(db_tweet[company_key], k_average)

    fig.add_trace(
        go.Scatter(
            x=[time_tweet[0] for time_tweet in averaged_sentiments],
            y=[time_tweet[1] for time_tweet in averaged_sentiments],
            type='scatter', 
            name='sentiment-data'
        ),
        secondary_y=True,
    )
    
    fig.update_layout(
        title_text='Stock Data for {}'.format(db_company[company_key][0])
    )
        
    return fig
    
    # return {
    #     'data': [
    #         {
    #             'x': [time_price[0] for time_price in db_price[company_key]], 
    #             'y': [time_price[1] for time_price in db_price[company_key]], 
    #             'type': 'line', 'name': 'stock-data'
    #         },
    #         {
    #             'x': [time_tweet[0] for time_tweet in db_tweet[company_key]],
    #             'y': [time_tweet[1] for time_tweet in db_tweet[company_key]],
    #             'type': 'scatter', 'name': 'sentiment-data'
    #         }
    #     ],
    #     'layout': {
    #         'title': 'Stock Data for {}'.format(db_company[company_key][0])
    #     }
    # }

if __name__ == '__main__':
    app.run_server(debug=True)