import requests
import json
import collections
import copy
import psycopg2
import time as tm
import datetime as dt
import threading
from credentials import loadCredentials

# Sample JSON Response from the Finnhub API.
# {'c': [257.2, 257.21, 257.69, 257.77, 257.75], 
# 'h': [257.2, 257.21, 257.69, 257.79, 257.8], 
# 'l': [257.2, 257.21, 257.3, 257.65, 257.73], 
# 'o': [257.2, 257.21, 257.3, 257.65, 257.75], 
# 's': 'ok', 't': [1572910200, 1572910260, 1572910440, 1572910500, 1572910560], 'v': [322, 625, 9894, 1480, 2250]}


# Global Vars
etf_list = ['XLY', 'XLV', 'XLF', 'XLK', 'XLB', 'XLI', 'XLU', 'XLE', 'XOP', 'XLP', 'XME', 'UNG', 'USO']
type_of_candle = '1MBar-AvgVolume2WK'

upper_bound_num_candles = 4500 # 2 weeks worth of 1 minute bars. Rough figure since each ETF returns slightly different data. 

# Three Unix Time Periods for our start times, if needed. For first run, we calculate from ETF inception, which varies based on
# Ticker. 
twenty_years_unix = 479779929
fifteen_years_unix = 410127933
uso_ung_twelve_years_unix = 402263096

# Increment 2 weeks in UNIX Standard Time
increment_time = 600000

lock = threading.Lock() # To help avoid API limits.

# Database Credentials and Finnhub.io Token (data source)
credentials = loadCredentials()
database_user = credentials["database"]["username"]
database_password = credentials["database"]["password"]
database_db = credentials["database"]["database"]
database_host = credentials["database"]["host"]
database_port = credentials["database"]["port"]
finnhub_token = credentials["finnhub"]["token"]

def createListForAverage(volume_list, start_time, end_time, etf):
    
    global upper_bound_num_candles
    calculate_average = True
    end_time = start_time
    start_time = start_time - increment_time
    candle_queue = collections.deque([])

    while calculate_average == True:

        lock.acquire()
        calculate_avg_candles = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        lock.release()
        avg_etf_candle = calculate_avg_candles.json()
        avg_etf_candle_vol = avg_etf_candle['v']

        if (len(avg_etf_candle_vol) + len(candle_queue) > upper_bound_num_candles):
            calculate_average = False
            break

        avg_etf_candle['v'].reverse()
        for volume in avg_etf_candle['v']:
            candle_queue.appendleft(volume)

        end_time = end_time - increment_time
        start_time = start_time - increment_time

    return candle_queue

def createCandles(etf):
    
    connection = psycopg2.connect(user = database_user,
            password = database_password,
            host = database_host,
            port = database_port,
            database = database_db)
    cursor = connection.cursor()
    select_query = f'select endtime from public.customcandle_lasttime where type = \'{type_of_candle}\''
    cursor.execute(select_query)
    stored_time = cursor.fetchone() # Fetch the end time value from our table if it exists. Point of restart for the script. Only ever one element here.
    cursor.close()
    connection.close()

    # If we have no time value stored, start with ETF Inception
    if (stored_time == None):
        # Calculate relevant ETF inception time. Varies slightly per ticker so need to generate some edge cases.
        time_to_inception = fifteen_years_unix if (etf == 'XOP' or etf == 'XME') else twenty_years_unix
        if (etf == 'UNG' or etf == 'USO'): time_to_inception = uso_ung_twelve_years_unix

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
    
    while stored_time >= start_time: # need this I think? how to handle last run? 
        # lock.acquire()
        get_candle = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        # lock.release()
        etf_candle = get_candle.json()
        
        # If we happen to find a 'no_data' but we are still loading, just continue.
        if (etf_candle['s'] == 'no_data'):
            # print(f'etf {etf} had no results at time period {start_time} to {end_time}') # Logging assistance

            # If we find a no_data but its also the last run, break the loop.
            if last_run == True:
                break
            # We want to ensure we run up until 
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


        # If its our first run, we want to decrement from the current time period and calculate an average for the present time period we are calculating.
        # We add ~2 weeks of candles to queue to calculate and manage the average for that time period.
        if isFirstRun:
            isFirstRun = False 
            candle_queue = createListForAverage(etf_candle['v'], start_time, end_time, etf)
            num_candles_for_avg = len(candle_queue)
            for vol in candle_queue:
                average_volume += int(vol)

        current_candle_count = 1
        current_volume = 0
        current_candle_high = 0.0 
        current_candle_low = 0.0
        for close, high, low, ope, volume, time in zip(etf_candle['c'], etf_candle['h'], etf_candle['l'], etf_candle['o'], etf_candle['v'], etf_candle['t']):
            # print('--- new loop item ---')
            

            average = int(average_volume / num_candles_for_avg)
            current_volume += int(volume)
            high = float(high)
            low = float(low)
            close = float(close)
            ope = float(ope)
            # print(f'added {volume} to current volume: {current_volume}. Current Average is {average} and will make new candle once current volume passes it.')

            # Calculate our high for the volume candle.
            current_candle_high = close if close > current_candle_high and ope < close else ope
            # Calculate our low for the volume candle.
            if (current_candle_low == 0.0):
                current_candle_low = ope if ope < close else close
            else:
                current_candle_low = close if close < current_candle_low and close < ope else ope

            # Average is the sum of the Candle Queue, representing two weeks of data, divided by its length. We want to create a new candle
            # every time volume hits that average. This is contrary to time-based candles where you make a new one every minute.
            # One volume candle may be three-1 minute bars one time, then seven the next. This if-block handles that.
            # The SQL Insert needs to be broken out into a new method.
            if (current_volume > average):
                current_candle_time = dt.datetime.utcfromtimestamp(time).strftime("%m/%d/%Y %H:%M")
                
                connection = psycopg2.connect(user = database_user,
                            password = database_password,
                            host = database_host,
                            port = database_port,
                            database = database_db)
                insert_args = (current_candle_time, ope, close, current_candle_high, current_candle_low,
                                etf, type_of_candle, current_volume)
                sql_insert = ''' 
                            insert into public.customcandle (enddate, open, close, high, low, ticker, type, candle_volume)
                            values (%s, %s, %s, %s, %s, %s, %s, %s)
                            
                            '''
                cursor = connection.cursor()
                cursor.execute(sql_insert, insert_args)
                connection.commit()
                cursor.close()
                connection.close()
                # print(f'New volume candle created using {current_candle_count} volume periods.') # Turn on for logging purpsoes

                # Reset all of our current values for the next candle.
                current_volume = 0
                current_candle_high = 0.0
                current_candle_low = 0.0
                current_candle_count = 1

            # If-block fails? Maintain our current count and move to next candle.
            else:
                # print('Did not create candle. Adding next candle to total volume.')
                current_candle_count += 1
            # End if block for inserting

            # We don't want to give us O(n^3) complexity with these loops by recalculating the total volume from the entire queue of 3000-4000 elements.
            # However, we want to maintain a queue of the last ~2 weeks to ensure our current candle is being created based on a
            # 'relevant' volume average. Volume in 2005 is a lot different than volume in 2020, so this gives the candles more merit.
            # Continue to maintain the queue, but subtract the removed element and add the new element to the sum. No need
            # to recalculate every time.
            remove_volume = candle_queue.popleft()
            average_volume -= remove_volume

            candle_queue.append(volume)
            average_volume += volume
            # print(f'appended {volume} to list and popped off: {remove_volume} for ETF: {etf}')
            # print('--- end candle ---')

        # Last stage of while Loop. After we loop through an entire API query, we increment time and do it again.
        if last_run == True:
            break
        if stored_time > (start_time + increment_time):
            # Increment 1 week. API only returns ~5000 rows, so we use shorter bursts.
            start_time = start_time + increment_time
            end_time = end_time + increment_time
        else:
            # If the stored time is higher, we want to use this as our start_time and call this our last run. This matters when we are running frequently
            # so that we do not run the script on data a second time.
            start_time = stored_time
            end_time = stored_time + increment_time
            last_run = True # Ensures when this block hits it is the last run.

        # end for loop
    # end while loop
# end function



def generateCandles():

    # Multithread our candle generation. Each ETF is independent so speeds up data entry because ~15-20 years takes awhile.
    # Create new thread for each process calling createCandles method. After they complete, join them all.
    threads = []
    for i in range(len(etf_list)):
        thread = threading.Thread(target=createCandles, args=(etf_list[i],))
        thread.start()
        threads.append(thread)
        print(f'started thread {i}')
        tm.sleep(10)
    
    for thread in threads:
        thread.join()


    connection = psycopg2.connect(user = database_user,
        password = database_password,
        host = database_host,
        port = database_port,
        database = database_db)
    cursor = connection.cursor()

    # Delete currently stored value of endtime. We only want one stored at a time.
    sql_delete = f'delete from public.customcandle_lasttime where type = \'{type_of_candle}\''
    cursor.execute(sql_delete)
    connection.commit()

    # Insert new value for endtime used on future/present day runs.
    end_time = tm.time()
    sql_insert = f'insert into public.customcandle_lasttime (endtime, type) values ({end_time}, \'{type_of_candle}\')'
    cursor.execute(sql_insert)
    connection.commit()

    cursor.close()
    connection.close()


def main():
    generateCandles()  

if __name__ == "__main__":
    main()