# Brandon Fisher
# CS 598 Fall 2019 Senior Project
#
# Dash application for data visualization

import dash
import dash_table
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
import psycopg2
from datetime import datetime, time as tme
from pytz import timezone
import time
import numpy as np
from collections import OrderedDict
import pandas as pd

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
            tweet_info[tweet[0]].append((tweet[1].astimezone(timezone('EST')), tweet[2]))
        else:
            tweet_info.update({tweet[0]: [(tweet[1].astimezone(timezone('EST')), tweet[2])]})
    return tweet_info

def KNeighborAverage(time_tweet_list, k):
    averaged_list = []
    i = 0
    while i < len(time_tweet_list) - k - 1:
        averaged_list.append(
            (
                time_tweet_list[i+int(k/2)][0], 
                sum([ts[1] for ts in time_tweet_list[i:i+k]]) / len(time_tweet_list[i:i+k])
            ))
        i += int(k/2)
    return averaged_list

def CreatePreAveragedList(db_tweet):
    averages_dict = {}
    for company in db_tweet.keys():
        averages_dict.update({
            company: {
                0: db_tweet[company],
                5: KNeighborAverage(db_tweet[company], 5),
                10: KNeighborAverage(db_tweet[company], 10),
                25: KNeighborAverage(db_tweet[company], 25)
            }
        })
    return averages_dict

db_company = GetDbCompany()
db_price = GetDbPrice()
db_tweet = GetDbTweet()
current_stats={}

for time_tweet_list in db_tweet.values():
    time_tweet_list.sort()

averaged_list = CreatePreAveragedList(db_tweet)

print('Processing Complete')

app = dash.Dash(__name__)

app.layout = html.Div(children=[

    html.Div(className='TitleArea', children=[
        html.H1(className='Title', children='Sentiment & Securities Analysis'),

        html.H3(className='Info', children='''
            Brandon Fisher - CS Senior Project, Kansas State University 2019, built with Dash
        '''),
    ]),

    html.Div(className='Interact', children=[

        html.H3(children='Company:'),
        
        dcc.Dropdown(
            id='ticker-select',
            className='TickerSelect',
            options=[
                {'label': db_company[company_id][1] + ' - ' + db_company[company_id][0], 'value': company_id} for company_id in db_company.keys()
            ], 
            value=11
        ),

        html.Div(className='OneDay', children=[
            html.H3(children='Select A Date:'),
            dcc.DatePickerSingle(
                id='date-picker-single',
                date=str(datetime(2019, 11,5))
            )
        ]),

        
        html.Div(className='ShowTweets', children=[
            html.H3(children='Show Tweets:'),
            
            dcc.RadioItems(
                id='show-tweets',
                className='ShowTweets',
                options=[
                    {'label': 'Yes', 'value': 1},
                    {'label': 'No', 'value': 0}
                ],
                value=0,
                style={
                    'color': 'white'
                }
            ),

        ]),

        html.Div(className='AverageTweets', children=[
            html.H3(children='Average Tweets in Groups of:'),
        
            dcc.Dropdown(
                id='sentiment-neighbor-average',
                className='SentimentNeighborAverage',
                options=[
                    {'label': 'None', 'value': 0},
                    {'label': '5', 'value': 5},
                    {'label': '10', 'value': 10},
                    {'label': '25', 'value': 25}
                ],
                value=0
            ),
        ]),
        
        html.Div(className='Statistics', id='statistics'),

        html.Div(className='ThresholdReturns', children=[
            dcc.Input(
                placeholder='Enter a Threshold',
                id='threshold',
                type='number'
            ),

            html.Div(className='Returns', id='Returns'),
        ])
    ]),



    html.Div(className='OneDayGraphArea', children=[
        dcc.Graph(id='one-day-stocks')
    ]),

    html.Div(className='DayStatsTable', children=[
        dash_table.DataTable(
            id='stats-table',
            columns=[
                {'name': 'Date', 'id': 'Date'},
                {'name': 'Open', 'id': 'Open'},
                {'name': 'Close', 'id': 'Close'},
                {'name': 'Change', 'id': 'Change'},
                {'name': 'Sentiment', 'id': 'Sentiment'}
            ],
            style_table={
                'overflowX': 'scroll',
                'width': '1400px',
            },
            fixed_rows={
                'headers': True,
                'data': 0
            },
            style_as_list_view=True,
            style_header={
                'backgroundColor': 'rgb(30, 30, 30)',
                'fontWeight': 'bold'
            },
            style_cell={
                'backgroundColor': 'rgb(50, 50, 50)',
                'color': 'white',
                'textAlign': 'left'
            },
            style_filter={
                'fontWeight': 'bold',
                'backgroundColor': 'rgb(120,120,120)'
            },   
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(60, 60, 60)'
                },
                {
                    'if': {
                        'column_id': 'Change',
                        'filter_query': '{Change} > 0'
                    },
                    'color': 'green'
                },
                {
                    'if': {
                        'column_id': 'Change',
                        'filter_query': '{Change} < 0'
                    },
                    'color': 'red'
                },
            ],
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
        ) 
    ]),
])
    

@app.callback(
    Output('one-day-stocks', 'figure'),
    [Input('ticker-select', 'value'),
    Input('date-picker-single', 'date'),
    Input('show-tweets', 'value'),
    Input('sentiment-neighbor-average', 'value')]
)
def update_one_day_graph(company_key, selected_date, show_tweets, k_average):
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    selected_date = datetime.strptime(selected_date.split(' ')[0], '%Y-%m-%d')

    fig.add_trace(
        go.Scatter(
            x=[time_price[0] for time_price in db_price[company_key] if time_price[0].date() == selected_date.date()], 
            y=[time_price[1] for time_price in db_price[company_key] if time_price[0].date() == selected_date.date()], 
            name='price',
            marker_color='rgb(35,135,35)'
        ),
        secondary_y=False,
    )

    averaged_sentiments = averaged_list[company_key][k_average]

    if show_tweets == 1:
        fig.add_trace(
            go.Scatter(
                x=[time_tweet[0] 
                    for time_tweet in averaged_sentiments
                    if time_tweet[0].date() == selected_date.date() and time_tweet[0].time() >= tme(hour=9, minute=30) and time_tweet[0].time() < tme(hour=16, minute=0)],
                y=[time_tweet[1] 
                    for time_tweet in averaged_sentiments
                    if time_tweet[0].date() == selected_date.date() and time_tweet[0].time() >= tme(hour=9, minute=30) and time_tweet[0].time() < tme(hour=16, minute=0)],
                type='scatter',
                mode='markers', 
                name='tweets'
            ),
            secondary_y=True,
        )

    fig.update_yaxes(title_text="<b>Stock Price</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Tweet Sentiment</b>", secondary_y=True)

    fig.update_layout(
        template='plotly_dark',
        title_text='<b>{} - {}</b>'.format(selected_date.strftime('%m/%d/%Y'), db_company[company_key][0]),
        xaxis_title='<b>Time</b>',
        
        autosize=False,
        width=800,
        height=500,
        margin=go.layout.Margin(
            l=100,
            r=100,
            b=50,
            t=50,
            pad=10
        )
    )

    return fig

@app.callback(
    Output(component_id='statistics', component_property='children'),
    [Input('ticker-select', 'value'),
    Input('date-picker-single', 'date')]
)
def update_statistics(company_key, selected_date):
    
    selected_date = datetime.strptime(selected_date.split(' ')[0], '%Y-%m-%d')
    price_change_pct = round((max([x for x in db_price[company_key]])[1] - 
                        min([x for x in db_price[company_key]])[1]) / min([x for x in db_price[company_key]])[1], 2)
    # This will probably work better with markdown
    return [
        html.Table([
            html.Tr([
                html.Td(style={
                        'width': '400px'
                    },
                    children='{} Cumulative Stats:'.format(db_company[company_key][1])
                ),
                html.Td(style={
                        'width': '400px'
                    },
                    children='{} on {}:'.format(db_company[company_key][1], selected_date.date())
                ),
            ]),
            html.Tr([
                html.Td(
                    html.Table([
                        html.Tr([
                            html.Td('Price Count:'),
                            html.Td(len(db_price[company_key]))
                        ]),
                        html.Tr([
                            html.Td('Tweet Count:'),
                            html.Td(len(averaged_list[company_key][0]))
                        ]),
                        html.Tr([
                            html.Td('Price Change:'),
                            html.Td('{}%'.format(price_change_pct))
                        ]),
                        html.Tr([
                            html.Td('Average Sentiment:'),
                            html.Td(
                                round(sum([x[1] for x in db_tweet[company_key]]) / 
                                len([x[1] for x in db_tweet[company_key]]), 4)
                            )
                        ])
                    ])
                ),
                html.Td(
                    html.Table([
                        html.Tr([
                            html.Td('Open:'),
                            html.Td(
                                'N/A' if len([x for x in db_price[company_key] if x[0].date() == selected_date.date()]) <= 0 else min([x for x in db_price[company_key] if x[0].date() == selected_date.date()])[1]
                            )
                        ]),
                        html.Tr([
                            html.Td('Close:'),
                            html.Td(
                                'N/A' if len([x for x in db_price[company_key] if x[0].date() == selected_date.date()]) <= 0 else max([x for x in db_price[company_key] if x[0].date() == selected_date.date()])[1]
                            )
                        ]),
                        html.Tr([
                            html.Td('Change:'),
                            html.Td(
                                'N/A' if len([x for x in db_price[company_key] if x[0].date() == selected_date.date()]) <= 0 else 
                                    '{}%'.format(round((max([x for x in db_price[company_key] if x[0].date() == selected_date.date()])[1] - 
                                            min([x for x in db_price[company_key] if x[0].date() == selected_date.date()])[1]) / 
                                            max([x for x in db_price[company_key] if x[0].date() == selected_date.date()])[1] * 100, 2))
                            )
                        ]),
                        html.Tr([
                            html.Td('Sentiment:'),
                            html.Td(
                                0.0 if len([x[1] for x in db_tweet[company_key] if x[0].date() == selected_date.date()]) <= 0 else
                                round(sum([x[1] for x in db_tweet[company_key] if x[0].date() == selected_date.date()]) / 
                                len([x[1] for x in db_tweet[company_key] if x[0].date() == selected_date.date()]), 4)
                            )
                        ])
                    ])
                )
            ])
        ])
    ]

@app.callback(
    Output('Returns', 'children'),
    [Input('ticker-select', 'value'),
    Input('threshold', 'value')]
)
def update_returns(company_key, threshold):
    if threshold is None:
        threshold = 0.0
    price_dates = list(set(x[0].date() for x in db_price[company_key]))
    price_dates.sort(reverse=True)
    tweet_dates = list(set(x[0].date() for x in db_tweet[company_key]))
    tweet_dates.sort(reverse=True)
    dates = [d for d in tweet_dates if d in price_dates]
    market_buy = min([x for x in db_price[company_key]])[1]
    market_sell = max([x for x in db_price[company_key]])[1]
    tweet_returns=[]
    for date in dates:
        date_sent = (sum([x[1] for x in db_tweet[company_key] if x[0].date() == date]) / 
            len([x[1] for x in db_tweet[company_key] if x[0].date() == date]))
        if date_sent > threshold:
            tweet_returns.append(
                max([x for x in db_price[company_key] if x[0].date() == date])[1] -
                min([x for x in db_price[company_key] if x[0].date() == date])[1]
            )
    return [
        html.H4('Between {} and {} with selected returns above: {}'.format(
            min(dates).strftime('%m-%d-%Y'),
            max(dates).strftime('%m-%d-%Y'),
            threshold
        )),
        html.H4('Market Return:    ${}'.format(round(market_sell - market_buy, 2))),
        html.H4('Selected Returns: ${}'.format(round(sum(tweet_returns), 2)))
    ]
    


@app.callback(
    Output('stats-table', 'data'),
    [Input('ticker-select', 'value')]
)
def update_stats_table(company_key):
    
    dates = list(set(x[0].date() for x in db_price[company_key]))
    dates.sort(reverse=True)

    data = OrderedDict(
        [
            ('Date', dates),
            ('Open', [min([x for x in db_price[company_key] if x[0].date() == date])[1] for date in dates]), 
            ('Close', [max([x for x in db_price[company_key] if x[0].date() == date])[1] for date in dates]),
            ('Change', [round(
                (max([x for x in db_price[company_key] if x[0].date() == date])[1] - 
                min([x for x in db_price[company_key] if x[0].date() == date])[1]) / 
                min([x for x in db_price[company_key] if x[0].date() == date])[1] * 100, 2) for date in dates]),
            ('Sentiment', [0.0 if len([x[1] for x in db_tweet[company_key] if x[0].date() == date]) <= 0 else
                            round(sum([x[1] for x in db_tweet[company_key] if x[0].date() == date]) / 
                                len([x[1] for x in db_tweet[company_key] if x[0].date() == date]), 4) for date in dates])
        ]
    )

    df = pd.DataFrame(data)

    return df.to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)