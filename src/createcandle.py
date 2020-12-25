import collections
import copy
import os
import datetime as dt
import json
import psycopg2
import pandas as pd
import numpy as np
import requests
import sqlalchemy
from sqlalchemy import create_engine
import time as tm
import threading

# Sample JSON Response from the Finnhub API.
# {'c': [257.2, 257.21, 257.69, 257.77, 257.75], 
# 'h': [257.2, 257.21, 257.69, 257.79, 257.8], 
# 'l': [257.2, 257.21, 257.3, 257.65, 257.73], 
# 'o': [257.2, 257.21, 257.3, 257.65, 257.75], 
# 's': 'ok', 't': [1572910200, 1572910260, 1572910440, 1572910500, 1572910560], 
# 'v': [322, 625, 9894, 1480, 2250]}

# Global Vars
etf_list = ['AMZN', 'XLY', 'XLV', 'XLF', 'XLK', 'XLB', 'XLI', 'XLU', 'XLE', 'XOP', 'XLP', 'XME', 'UNG'] # , 'USO'
type_of_candle = '1MBar-AvgVolume2WK'
upper_bound_num_candles = 3500 # 2 weeks worth of 1 minute bars. Rough figure since each ETF returns slightly different data. 
# Three Unix Time Periods for our start times, if needed. For first run, we calculate from ETF inception, which varies based on Ticker.
twenty_years_unix = 479779929
fifteen_years_unix = 407127933
uso_ung_twelve_years_unix = 400263096
increment_time = 600000 # Increment 2 weeks in UNIX Standard Time

# Database Credentials and Finnhub.io Token (data source)
# credentials = loadCredentials()
# database_user = credentials["database"]["username"]
# database_password = credentials["database"]["password"]
# database_db = credentials["database"]["database"]
# database_host = credentials["database"]["host"]
# database_port = credentials["database"]["port"]
# finnhub_token = credentials["finnhub"]["token"]

database_user = os.environ["db_username"]
database_password = os.environ["db_password"]
database_db = os.environ["db_database"]
database_host = os.environ["db_host"]
database_port = os.environ["db_port"]
finnhub_token = os.environ["finnhub_token"]

# Connect to our Postgres RDS. 
def rds_connect():
    return psycopg2.connect(user = database_user,
                            password = database_password,
                            host = database_host,
                            port = database_port,
                            database = database_db)

# Location the max length of the dataframe and append our candle to the end.
def store_row(df, row):
    insert_loc = df.index.max()

    if pd.isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc + 1] = row

# Function to increment our time interval to continue generating candles. Needed because of API limits
# and we can't load many years of data at a time. Downsides of free resources.
def update_time_interval(last_run, start_time, end_time, increment_time, stored_time):
    # We want to ensure we run up until present day.
    if stored_time > (start_time + increment_time):
        # Increment 2 weeks. API only returns ~5000 rows, so we use shorter bursts.
        start_time = start_time + increment_time
        end_time = end_time + increment_time
        last_run = False
    else:
        # If the stored time is higher, we want to use this as our start_time and call this our last run. This matters when we are running frequently
        # so that we do not run the script on data a second time.
        start_time = stored_time
        end_time = stored_time + increment_time
        last_run = True # Ensures when this block hits it is the last run.

    return last_run, start_time, end_time

# Method to calculate our Average value, which we will use as a threshold to generate the new Volume Candle.
# We take the time we want to start running the job, and subtract from it. We return the queue of these elements.
def generateAverage(start_time, end_time, etf):

    calculate_average = True
    end_time = start_time
    start_time = start_time - increment_time
    candle_queue = collections.deque([])
    lookback_max = 10 # Max lookback period to handle small activity periods.
    current_lookback = 0

    while calculate_average == True:
        calculate_avg_candles = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        avg_etf_candle = calculate_avg_candles.json()
        tm.sleep(1)
        # Random time periods will not return data. We know our time periods are selected after ETF origin, so just continue. 
        if (avg_etf_candle['s'] == 'no_data'):
            end_time -= increment_time
            start_time -= increment_time
            current_lookback+=1
            tm.sleep(6) # Pause to not overload API calls.
            continue

        # We want to count up to about 3500 candles. Any more felt redundant, though this may be modified. If length of the current JSON return
        # is more than the "upper bound" variable, break from this loop. 
        avg_etf_candle_vol = avg_etf_candle['v']
        if len(avg_etf_candle_vol) + len(candle_queue) > upper_bound_num_candles or current_lookback >= lookback_max:
            calculate_average = False
            break
        # We're decrementing backwards until roughly the 2 week mark. In order to keep our data effective and valuable, we must keep it in order.
        # We take a JSON response that is very near our start time. The last element in the response is the *closest* to our start time. We want to ensure
        # that is at the furthest right on the queue. In order to do that, we reverse the list, and appendleft. Appendleft puts the following elements
        # behind the first one, which keeps things in order.
        avg_etf_candle['v'].reverse()
        for volume in avg_etf_candle['v']:
            candle_queue.appendleft(volume)

        end_time = end_time - increment_time
        start_time = start_time - increment_time
        current_lookback += 1
    # End While Loop.
    return candle_queue

# Function that creates the volume candle. Loop until we surpass the two week value average, and then create a candle for insert.
def create_vol_candle(etf, etf_candle, df, avg_volume_size, average_volume, candle_queue):
    current_candle_count = 1
    current_volume = 0
    current_candle_high = 0.0 
    current_candle_low = 0.0
    first_candle_open = 0.0

    # Break this into functin once we're certain all the details are ironed out.
    for close, high, low, open_, volume, time in zip(etf_candle['c'], etf_candle['h'], etf_candle['l'], etf_candle['o'], etf_candle['v'], etf_candle['t']):
        average = int(average_volume / avg_volume_size)
        current_volume += int(volume)
        high = float(high)
        low = float(low)
        close = float(close)
        open_ = float(open_)
        # We want our Volume Candle, which may represent many time candles, to have its open and close be the *open of the first candle* 
        # and the *close of the last candle*. 
        if (first_candle_open == 0.0):
            first_candle_open = open_

        # Calculate our lowest and highest value for the volume candle.
        current_candle_high = close if close > current_candle_high and close >= open_ else current_candle_high
        current_candle_high = open_ if open_ > current_candle_high and open_ > close else current_candle_high
        if current_candle_low == 0.0:
            current_candle_low = open_ if open_ < close else close
        else:
            current_candle_low = close if close < current_candle_low and close < open_ else current_candle_low
            current_candle_low = open_ if open_ < current_candle_low and open_ < close else current_candle_low

        # Average is the sum of the Candle Queue, representing two weeks of data, divided by its length. We want to create a new candle
        # every time volume hits that average. This is contrary to time-based candles where you make a new one every minute.
        # One volume candle may be three-1 minute bars one time, then seven the next. This if-block handles that creation and stores in a dataframe.
        if current_volume > average:
            current_candle_time = dt.datetime.utcfromtimestamp(time).strftime("%m/%d/%Y %H:%M")
            insert_args = (current_candle_time, first_candle_open, close, current_candle_high, current_candle_low,
                            etf, type_of_candle, current_volume)
            store_row(df, insert_args)

            # Reset all of our current values for the next candle.
            current_volume = 0
            current_candle_high = 0.0
            current_candle_low = 0.0
            first_candle_open = 0.0
            current_candle_count = 1

        # If-block fails? Maintain our current count and move to next time candle.
        else:
            current_candle_count += 1
        # End if block for inserting
        
        # Update our queue with the most recent value, and pop off the oldest.
        remove_volume = candle_queue.popleft()
        average_volume -= remove_volume
        candle_queue.append(volume)
        average_volume += volume
        # end For loop
    return df

# Method to instantiate the time periods we're working with.
# Calculate relevant ETF inception time. Varies slightly per ticker so need to generate some edge cases. Looking to migrate this to SQL Table instead of
# global vars. 
def instantiate_time_period(etf, stored_time):

    # If we have no time value stored, start with ETF Inception Date.
    if (stored_time == None):
        time_to_inception = fifteen_years_unix if (etf == 'XOP' or etf == 'XME') else twenty_years_unix
        if (etf == 'UNG' or etf == 'USO'): 
            time_to_inception = uso_ung_twelve_years_unix

        end_time = int(tm.time()) - time_to_inception
        start_time = end_time - increment_time
        stored_time = int(tm.time())
    # Else this job has been run before and grab the stored time value from the DB. 
    else:
        stored_time = int(stored_time[0])
        end_time = stored_time + increment_time
        start_time = stored_time   
        stored_time = int(tm.time())
    return end_time, start_time, stored_time

# Check our secondary database table to see if we have a 'last time this was run' element.
def fetch_stored_time(etf):
    connection = rds_connect()
    cursor = connection.cursor()
    select_query = f'select endtime from public.customcandle_lasttime where type = \'{type_of_candle}\' and etf = \'{etf}\''
    cursor.execute(select_query)
    stored_time = cursor.fetchone() # Fetch the end time value from our table if it exists. Point of restart for the script. Only ever one element here.
    cursor.close()
    connection.close()
    return stored_time

# Function that pulls from our datasource, initiates the average volume calculation, and calls the function to actually generate
# the volume candle.
def prepare_candle(etf):

    # Instantiate our variables. Two bools to check if its our first/last run, our time periods to work with, a queue for storing recent datapoints,
    # and a dataframe for insertion.
    stored_time = fetch_stored_time(etf)
    end_time, start_time, stored_time = instantiate_time_period(etf, stored_time)    
    last_run = False
    first_run = True
    candle_queue = collections.deque([])
    avg_volume_size = 0
    average_volume = 0 
    df_columns = ["enddate", "open", "close", "high", "low", "ticker", "type", "candle_volume"]
    df = pd.DataFrame(columns=df_columns)

    # If its our first run, we want to decrement from the start time period and calculate an average. 
    # We are using 1 minute bars, so add ~2 weeks of candles to a queue. Queue stores our candle average size.
    if first_run:
        first_run = False 
        candle_queue = generateAverage(start_time, end_time, etf)
        avg_volume_size = len(candle_queue)
        for vol in candle_queue:
            average_volume += int(vol)
    
    while stored_time >= start_time:
        get_candle = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        etf_candle = get_candle.json()
        
        # If we happen to find a 'no_data' but we are still loading data, just continue. Its a one-off issue with datasource.
        if (etf_candle['s'] == 'no_data'):
            # If we find a no_data but its also the last run, break the loop.
            if last_run == True:  
                break
            else:
                last_run, start_time, end_time = update_time_interval(last_run, start_time, end_time, increment_time, stored_time)
                tm.sleep(5) # Pause to not overload API.
                continue

        # Loop through our JSON object to create the candles.
        df = create_vol_candle(etf, etf_candle, df, avg_volume_size, average_volume, candle_queue)

        # Last stage of while Loop. After we loop through an entire API query, we store the df, increment time and do it again.
        engine = create_engine(f'postgresql://{database_user}:{database_password}@{database_host}:{database_port}/{database_db}')
        con = engine.connect()
        df.to_sql('customcandle', engine, schema='public', index=False, if_exists="append")
        con.close()
        engine.dispose()
        df = df.iloc[0:0]

        if last_run == True:
            break
        else:
            last_run, start_time, end_time = update_time_interval(last_run, start_time, end_time, increment_time, stored_time)
    # end while loop
# end function

# Function to loop through the ETFs and store our "restart" elements in the RDS.
def generate_candles():

    for etf in etf_list:
        prepare_candle(etf)

        connection = rds_connect()
        cursor = connection.cursor()
        # Delete currently stored value of endtime. We only want one stored at a time.
        sql_delete = f'delete from public.customcandle_lasttime where type = \'{type_of_candle}\' and etf = \'{etf}\''
        cursor.execute(sql_delete)
        connection.commit()

        # Insert new value for endtime used on future/present day runs.
        end_time = tm.time()
        sql_insert = f'insert into public.customcandle_lasttime (endtime, type, etf) values ({end_time}, \'{type_of_candle}\', \'{etf}\')'
        cursor.execute(sql_insert)
        connection.commit()
        cursor.close()
        connection.close()
        print(f'Completed {etf}')

# def main():
#     generate_candles()  

# if __name__ == "__main__":
#     main()

# Handler
def my_handler(event, context):
    generate_candles()  

# if __name__ == "__main__":
#     main()