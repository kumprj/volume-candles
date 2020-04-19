import requests
import json
import collections
import copy
import psycopg2
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import time as tm
import datetime as dt
import threading
from credentials import loadCredentials

# Sample JSON Response from the Finnhub API.
# {'c': [257.2, 257.21, 257.69, 257.77, 257.75], 
# 'h': [257.2, 257.21, 257.69, 257.79, 257.8], 
# 'l': [257.2, 257.21, 257.3, 257.65, 257.73], 
# 'o': [257.2, 257.21, 257.3, 257.65, 257.75], 
# 's': 'ok', 't': [1572910200, 1572910260, 1572910440, 1572910500, 1572910560], 
# 'v': [322, 625, 9894, 1480, 2250]}


# Global Vars
etf_list = ['XLY', 'XLV', 'XLF', 'XLK', 'XLB', 'XLI', 'XLU', 'XLE', 'XOP', 'XLP', 'XME', 'UNG', 'USO']
# etf_list = ['XLI', 'XLU', 'XLE', 'XOP', 'XLP', 'XME', 'UNG', 'USO']
type_of_candle = '1MBar-AvgVolume2WK'
  
upper_bound_num_candles = 3500 # 2 weeks worth of 1 minute bars. Rough figure since each ETF returns slightly different data. 

# Three Unix Time Periods for our start times, if needed. For first run, we calculate from ETF inception, which varies based on
# Ticker. 
twenty_years_unix = 479779929
fifteen_years_unix = 407127933
uso_ung_twelve_years_unix = 400263096
# Increment 2 weeks in UNIX Standard Time
increment_time = 600000

# lock = threading.Lock() # To help avoid API limits.

# Database Credentials and Finnhub.io Token (data source)
credentials = loadCredentials()
database_user = credentials["database"]["username"]
database_password = credentials["database"]["password"]
database_db = credentials["database"]["database"]
database_host = credentials["database"]["host"]
database_port = credentials["database"]["port"]
finnhub_token = credentials["finnhub"]["token"]


def rds_connect():
    return psycopg2.connect(user = database_user,
        password = database_password,
        host = database_host,
        port = database_port,
        database = database_db)


def store_row(df, row):
    insert_loc = df.index.max()

    if pd.isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc + 1] = row

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
        print(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        avg_etf_candle = calculate_avg_candles.json()

        # Random time periods will not return data. We know our time periods are selected after ETF origin, so just continue. 
        if (avg_etf_candle['s'] == 'no_data'):
            end_time -= increment_time
            start_time -= increment_time
            current_lookback+=1
            tm.sleep(2) # Pause to not overload API calls.
            continue

        # We want to count up to about 3500 candles. Any more felt redundant, though this may be modified. If length of the current JSON return
        # is more than the "upper bound" variable, break from this loop. 
        avg_etf_candle_vol = avg_etf_candle['v']
        if len(avg_etf_candle_vol) + len(candle_queue) > upper_bound_num_candles or current_lookback >= lookback_max:
            calculate_average = False
            break
        # We're decrementing backwards until roughly the 2 week mark. In order to keep our data effective and valuable, we must keep it in order.
        # We take a JSON response that is very near our start time. The last element in the response is the *closest* to our start time. We want to ensure
        # that is at the furthest right on the queue. In order to do that, we reverse the list, and appendleft. Appendleft basically puts the following elements
        # behind the first one, which keeps things in order.
        avg_etf_candle['v'].reverse()
        for volume in avg_etf_candle['v']:
            candle_queue.appendleft(volume)

        end_time = end_time - increment_time
        start_time = start_time - increment_time
        current_lookback+=1

    return candle_queue

def createCandles(etf):
    
    connection = rds_connect()
    cursor = connection.cursor()
    select_query = f'select endtime from public.customcandle_lasttime where type = \'{type_of_candle}\' and etf = \'{etf}\''
    cursor.execute(select_query)
    stored_time = cursor.fetchone() # Fetch the end time value from our table if it exists. Point of restart for the script. Only ever one element here.
    cursor.close()
    connection.close()

    # If we have no time value stored, start with ETF Inception
    if (stored_time == None):
        # Calculate relevant ETF inception time. Varies slightly per ticker so need to generate some edge cases.
        time_to_inception = fifteen_years_unix if (etf == 'XOP' or etf == 'XME') else twenty_years_unix
        if (etf == 'UNG' or etf == 'USO'): 
            time_to_inception = uso_ung_twelve_years_unix

        end_time = int(tm.time()) - time_to_inception
        start_time = end_time - increment_time
        stored_time = int(tm.time()) - increment_time
    # Else this job has been run before and grab the stored time value from the DB. 
    else:
        stored_time = int(stored_time[0])
        end_time = int(tm.time())
        start_time = stored_time

    # Variables for the while Loop.
    last_run = False
    candle_queue = collections.deque([])
    isFirstRun = True
    num_candles_for_avg = 0
    average_volume = 0 
    df_columns = ["enddate", "open", "close", "high", "low", "ticker", "type", "candle_volume"]
    df = pd.DataFrame(columns=df_columns)
    # If its our first run, we want to decrement from the start time period and calculate an average. 
    # We are using 1 minute bars, so add ~2 weeks of candles to a queue. Queue stores our candle average size.
    if isFirstRun:
        isFirstRun = False 
        candle_queue = generateAverage(start_time, end_time, etf)
        num_candles_for_avg = len(candle_queue)
        print(f'got out for etf {etf} ')
        for vol in candle_queue:
            average_volume += int(vol)

    # Large Loops could probably be optimized into functions. Once code is tested, refactor for this.


    while stored_time >= start_time:
        get_candle = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        etf_candle = get_candle.json()
        
        # If we happen to find a 'no_data' but we are still loading data, just continue. Its a one-off.
        if (etf_candle['s'] == 'no_data'):
            print(f'etf {etf} had no results at time period {start_time} to {end_time}') # Logging assistance
            tm.sleep(1) # Pause to not overload
            # If we find a no_data but its also the last run, break the loop.
            if last_run == True:
                break
            # We want to ensure we run up until present day.
            if stored_time > (start_time + increment_time):
                # Increment 2 weeks. API only returns ~5000 rows, so we use shorter bursts.
                start_time = start_time + increment_time
                end_time = end_time + increment_time
                continue
            else:
                # If the stored time is higher, we want to use this as our start_time and call this our last run. This matters when we are running frequently
                # so that we do not run the script on data a second time.
                start_time = stored_time
                end_time = stored_time + increment_time
                last_run = True # Ensures when this block hits it is the last run.
                continue

        current_candle_count = 1
        current_volume = 0
        current_candle_high = 0.0 
        current_candle_low = 0.0
        first_candle_open = 0.0

        # Break this into functin once we're certain all the details are ironed out.
        for close, high, low, open_, volume, time in zip(etf_candle['c'], etf_candle['h'], etf_candle['l'], etf_candle['o'], etf_candle['v'], etf_candle['t']):
            # print('--- new loop item ---')
            average = int(average_volume / num_candles_for_avg)
            current_volume += int(volume)
            high = float(high)
            low = float(low)
            close = float(close)
            open_ = float(open_)
            # We want our Volume Candle, which may represent many time candles, to have its open and close be the *open of the first candle* 
            # and the *close of the last candle*. 
            if (first_candle_open == 0.0):
                first_candle_open = open_

            # print(f'added {volume} to current volume: {current_volume}. Current Average is {average} and will make new candle once current volume passes it.')

            # Calculate our highest value for the volume candle.
            current_candle_high = close if close > current_candle_high and close >= open_ else current_candle_high
            current_candle_high = open_ if open_ > current_candle_high and open_ > close else current_candle_high
            # Calculate our lowest for the volume candle.
            if current_candle_low == 0.0:
                current_candle_low = open_ if open_ < close else close
            else:
                current_candle_low = close if close < current_candle_low and close < open_ else current_candle_low
                current_candle_low = open_ if open_ < current_candle_low and open_ < close else current_candle_low

            # Average is the sum of the Candle Queue, representing two weeks of data, divided by its length. We want to create a new candle
            # every time volume hits that average. This is contrary to time-based candles where you make a new one every minute.
            # One volume candle may be three-1 minute bars one time, then seven the next. This if-block handles that.
            # The SQL Insert needs to be broken out into a new method.
            if current_volume > average:
                current_candle_time = dt.datetime.utcfromtimestamp(time).strftime("%m/%d/%Y %H:%M")
                insert_args = (current_candle_time, first_candle_open, close, current_candle_high, current_candle_low,
                                etf, type_of_candle, current_volume)
                store_row(df, insert_args)
                
                # print(f'New volume candle created using {current_candle_count} volume periods.') # Turn on for logging purpsoes

                # Reset all of our current values for the next candle.
                current_volume = 0
                current_candle_high = 0.0
                current_candle_low = 0.0
                first_candle_open = 0.0
                current_candle_count = 1
                

            # If-block fails? Maintain our current count and move to next time candle.
            else:
                # print('Did not create candle. Adding next candle to total volume.')
                current_candle_count += 1
            # End if block for inserting
            
            # End of For Loop:
            # We don't want to give us O(n^3) complexity with these loops by recalculating the total volume from the entire queue of 3000-4000 elements.
            # However, we want to maintain a queue of the last ~2 weeks to ensure our current candle is being created based on a
            # 'relevant' volume average. Volume in 2005 is a lot different than volume in 2020, so this gives the candles more merit.
            # Continue to maintain the queue, but subtract the removed element and add the new element to the sum. No need
            # to recalculate every time. We popleft because that is the oldest element, and append on the right to label it the newest.
            remove_volume = candle_queue.popleft()
            average_volume -= remove_volume

            candle_queue.append(volume)
            average_volume += volume
            # print(f'appended {volume} to list and popped off: {remove_volume} for ETF: {etf}')
            # print('--- end candle ---')
            # end For loop

        # Last stage of while Loop. After we loop through an entire API query, we store the df, increment time and do it again.
        engine = create_engine(f'postgresql://{database_user}:{database_password}@{database_host}:{database_port}/{database_db}')
        con = engine.connect()
        df.to_sql('customcandle', engine, schema='public', index=False, if_exists="append")
        con.close()
        engine.dispose()
        df.iloc[0:0]
        if last_run == True:
            break
        if stored_time > (start_time + increment_time):
            # Increment. API only returns ~5000 rows, so we use shorter bursts of time.
            start_time = start_time + increment_time
            end_time = end_time + increment_time
        else:
            # If the stored time is higher, we want to use this as our start_time and call this our last run. This matters when we are running frequently
            # so that we do not run the script on data a second time.
            start_time = stored_time
            end_time = stored_time + increment_time
            last_run = True # Ensures when this block hits it is the last run.
        
    # end while loop
# end function



def generateCandles():

    # Multithread our candle generation. Each ETF is independent so speeds up data entry because ~15-20 years takes awhile.
    # Create new thread for each process calling createCandles method. After they complete, join them all.
    for etf in etf_list:
        createCandles(etf)

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
    # threads = []
    # for i in range(5):
    #     thread = threading.Thread(target=createCandles, args=(etf_list[i],))
    #     thread.start()
    #     threads.append(thread)
    #     print(f'started thread {i}')
    #     tm.sleep(10)
    # for thread in threads:
    #     thread.join()
    # threads = []
    # for i in range(5,9):
    #     thread = threading.Thread(target=createCandles, args=(etf_list[i],))
    #     thread.start()
    #     threads.append(thread)
    #     print(f'started thread {i}')
    #     tm.sleep(10)
    # for thread in threads:
    #     thread.join()
    # threads = []
    # for i in range(8,13):
    #     thread = threading.Thread(target=createCandles, args=(etf_list[i],))
    #     thread.start()
    #     threads.append(thread)
    #     print(f'started thread {i}')
    #     tm.sleep(10)
    # for thread in threads:
    #     thread.join()

    # for i in range(len(etf_list)):
    #     thread = threading.Thread(target=createCandles, args=(etf_list[i],))
    #     thread.start()
    #     threads.append(thread)
    #     print(f'started thread {i}')
    #     tm.sleep(10)
    
    # for thread in threads:
    #     thread.join()


    # connection = rds_connect()
    # cursor = connection.cursor()

    # # Delete currently stored value of endtime. We only want one stored at a time.
    # sql_delete = f'delete from public.customcandle_lasttime where type = \'{type_of_candle}\''
    # cursor.execute(sql_delete)
    # connection.commit()

    # # Insert new value for endtime used on future/present day runs.
    # end_time = tm.time()
    # sql_insert = f'insert into public.customcandle_lasttime (endtime, type) values ({end_time}, \'{type_of_candle}\')'
    # cursor.execute(sql_insert)
    # connection.commit()

    # cursor.close()
    # connection.close()


def main():
    generateCandles()  

if __name__ == "__main__":
    main()