import requests
import json
import collections
import copy
import psycopg2
import time as tm
import datetime as dt
from credentials import loadCredentials

# etf_list = ['xly', 'xlv', 'xlf', 'xlk', 'xlb', 'xli', 'xlb', 'xlu', 'xle', 'xop', 'xlp', 'xme']
    # Sample JSON
    # {'c': [257.2, 257.21, 257.69, 257.77, 257.75], 
    # 'h': [257.2, 257.21, 257.69, 257.79, 257.8], 
    # 'l': [257.2, 257.21, 257.3, 257.65, 257.73], 
    # 'o': [257.2, 257.21, 257.3, 257.65, 257.75], 
    # 's': 'ok', 't': [1572910200, 1572910260, 1572910440, 1572910500, 1572910560], 'v': [322, 625, 9894, 1480, 2250]}
    # 43200 minutes
# cd /c/Users/robbi/Documents/Projects/aws/python_code/volume_candle
# Global Vars
etf_list = ['XLY', 'XLV', 'XLF', 'XLK', 'XLB', 'XLI', 'XLU', 'XLE', 'XOP', 'XLP', 'XME']
type_of_candle = '1MBar-AvgVolume90D'
# subtract_90_time = 2592000 # For job restarts, to recalculate our 90 day average
# num_candles_for_avg = 43200
upper_bound_num_candles = 3500
num_candles_for_avg = 0
twenty_years_unix = 479779929
fifteen_years_unix = 410127933
# increment_time = 15897600
increment_time = 500000
isFirstRun = True
candle_queue = collections.deque([])
# 2005 01 01: 1104537600
# 2004 07 01: 1088640000
# 3/21/2020 revert back to d32560e1ad6c8602b2c71b3f93e2e41db3fe2d7c if needed for *almost* working



credentials = loadCredentials()
database_user = credentials["database"]["username"]
database_password = credentials["database"]["password"]
database_db = credentials["database"]["database"]
database_host = credentials["database"]["host"]
database_port = credentials["database"]["port"]
finnhub_token = credentials["finnhub"]["token"]

def createListForAverage(volume_list, start_time, end_time, etf):
    global isFirstRun, candle_queue, num_candles_for_avg, upper_bound_num_candles
    print("First run, calculating average before proceeding.")
    calculate_average = True
    end_time = start_time
    start_time = start_time - increment_time

    # Reset queue to blank for each etf
    candle_queue = collections.deque([])
    while calculate_average == True:
        
        calculate_avg_candles = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        avg_etf_candle = calculate_avg_candles.json()

        # print('2')
        # print(start_time)
        # print(end_time)
        isFirstRun = False    
        # i = 0
        # print(avg_etf_candle['v'])

        avg_etf_candle_vol = avg_etf_candle['v']
        if (len(avg_etf_candle_vol) + len(candle_queue) > upper_bound_num_candles):
            calculate_average = False
            break
        # print(f' len volume list {len(avg_etf_candle_vol)}')

        avg_etf_candle['v'].reverse()
        # print(avg_etf_candle['v'])
        # candle_queue = collections.deque([])
        for volume in avg_etf_candle['v']:
            # if (i == num_candles_for_avg):
            #     break
            candle_queue.appendleft(volume)
            # i += 1

        end_time = end_time - increment_time
        start_time = start_time - increment_time
         

    num_candles_for_avg = len(candle_queue)

def createCandles(etf):
    
    connection = psycopg2.connect(user = database_user,
            password = database_password,
            host = database_host,
            port = database_port,
            database = database_db)
    cursor = connection.cursor()
    select_query = 'select endtime from public.customcandle_lasttime where type = \'1MBar-AvgVolume90D\''
    cursor.execute(select_query)
    stored_time = cursor.fetchone() # int([0]) # This returns as a tuple (xx,) <-- since we're only ever storing one value, just grab position 0.
    # print(f'stored_time {stored_time}')
    cursor.close()
    connection.close()

    # If we have no data stored, start with 20 years ago.
    if (stored_time == None):
        # print('hi')
        time_to_inception = fifteen_years_unix if (etf == 'XOP' or etf == 'XME') else twenty_years_unix
        end_time = int(tm.time()) - time_to_inception
        start_time = end_time - increment_time
        stored_time = int(tm.time()) - increment_time
    else:
        stored_time = int(stored_time[0])
        end_time = int(tm.time())
        start_time = stored_time
        # print(f'hello {stored_time}')
    # print('1')
    # print(start_time)
    # print(end_time)
    # start_time = end_time - increment_time
    # start_time = stored_time if (stored_time > start_time) else start_time
    isFirstRun = True
    last_run = False
    average_volume = 0
    # if (isFirstRun): start_time = start_time - subtract_90_time # Go back 90 days so we can calculate 90 day avg if its the first run
    
    # print(f'start time {start_time}')
    # print(f'end time {end_time}')
    
    while stored_time >= start_time: # need this I think? how to handle last run? 
        get_candle = requests.get(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}')
        etf_candle = get_candle.json()

        # If we don't return any data, break. Error handling for if we hit times that are equal to each other.
        if (etf_candle['s'] == 'no_data'):
            stored_time = start_time + 1
            break
        # print(f'https://finnhub.io/api/v1/stock/candle?symbol={etf}&resolution=1&from={start_time}&to={end_time}&token={finnhub_token}') # 2019 01 01: 1546300800
                                                                                                                                            # 2018 07 01 1530403200 #05 01: 1525132800
        # print(etf_candle)
        # x = len(etf_candle['v'])
        # print(x)

        if isFirstRun: 
            createListForAverage(etf_candle['v'], start_time, end_time, etf)
            for vol in candle_queue:
                average_volume += int(vol)
        # print('3')
        # print(start_time)
        # print(end_time)


        # if isFirstRun: createListForAverage(etf_candle['v'])
        # print(f'candle_queue size {len(candle_queue)}')
        current_candle_count = 1
        current_volume = 0
        
        for close, high, low, ope, volume, time in zip(etf_candle['c'], etf_candle['h'], etf_candle['l'], etf_candle['o'], etf_candle['v'], etf_candle['t']):
            print('--- new loop item ---')
            # current_volume = 0 # This line can go I think
            
            current_close_high = 0.0 
            current_close_low = 0.0

            # for vol in candle_queue:
            #     average_volume += int(vol)
            
            # sprint(average_volume)
            average = int(average_volume / num_candles_for_avg)
            # print(f'Current avg {average} ')
            # print(f'ninety day volume: {average_volume}')
            # average_volume -= last_val
            current_volume += int(volume)
            print(f'added {volume} to current volume: {current_volume}. Current Average is {average} and will make new candle once current volume passes it.')
            high = float(high)
            low = float(low)
            close = float(close)
            ope = float(ope)

            if (ope > current_close_high):
                current_close_high = ope
            if (close > current_close_high and ope < close):
                current_close_high = close

            if (close < current_close_low or current_close_low == 0.0):
                if (current_close_low == 0.0 and ope < close):
                    current_close_low = ope
                else:
                    current_close_low = close

            if (ope < close and ope < current_close_low and ope != 0.0):
                current_close_low = ope

            if (current_volume > average):
                current_candle_time = dt.datetime.utcfromtimestamp(time).strftime("%m/%d/%Y %H:%M")
                
                connection = psycopg2.connect(user = database_user,
                            password = database_password,
                            host = database_host,
                            port = database_port,
                            database = database_db)
                insert_args = (current_candle_time, ope, close, current_close_high, current_close_low,
                                etf, type_of_candle, current_volume) # str(average alternative to type_of_candle for testing
                sql_insert = ''' 
                            insert into public.customcandle (enddate, open, close, high, low, ticker, type, candle_volume)
                            values (%s, %s, %s, %s, %s, %s, %s, %s)
                            
                            '''
                cursor = connection.cursor()
                cursor.execute(sql_insert, insert_args)
                connection.commit()
                cursor.close()
                connection.close()
                print(f'New volume candle created using {current_candle_count} volume periods.') # for logging purpsoes
                # Reset all of our values
                current_volume = 0
                current_close_high = 0.0
                current_close_low = 0.0
                current_candle_count = 1
            else:
                print('Did not create candle. Adding next candle to total volume.')
                current_candle_count += 1
            # End if block for inserting

            # average_volume += volume
            # last_val = volume
            # Add current element to queue for recalculating new average 
            # print(f'size before {len(candle_queue)}')
            remove_volume = candle_queue.popleft()
            average_volume -= remove_volume
            # # logging.debug(f'{len(candle_queue)}')
            # candle_queue.pop()
            # # logging.debug(f'popped off {x}')
            # # logging.debug(candle_queue.index(0))
            candle_queue.append(volume)
            average_volume += volume
            # start_time = start_time + increment_time
            # end_time = end_time + increment_time
            # print(candle_queue.index(0))
            # print(f'appended {volume}')
            # print(volume)
            # y = candle_queue.popleft()
            print(f'appended {volume} to list and popped off: {remove_volume} for ETF: {etf}')
            print('--- end ---')

        # Last stage of while Loop.
        if last_run == True:
            break
        if stored_time > (start_time + increment_time):
            # Decrement back 6 months. API only returns 50000 rows. 6 month increments is a safe amount.
            start_time = start_time + increment_time
            end_time = end_time + increment_time
        else:
            # If the stored time is higher, we want to use this as our start_time and call this our last run. This matters when we are running frequently
            # so that we do not run the script on data a second time.
            start_time = stored_time
            last_run = True # Ensures when this block hits it is the last run.

        # end for loop
    # end while loop
    connection = psycopg2.connect(user = database_user,
        password = database_password,
        host = database_host,
        port = database_port,
        database = database_db)
    cursor = connection.cursor()

    # Delete currently stored value of endtime
    # sql_delete = 'delete from public.customcandle_lasttime where type = %s'
    # delete_args = type_of_candle
    # cursor.execute(sql_delete, delete_args)
    sql_delete = f'delete from public.customcandle_lasttime where type = \'{type_of_candle}\''
    cursor.execute(sql_delete)
    connection.commit()

    # Insert new value of endtime for future runs
    # sql_insert = 'insert into public.customcandle_lasttime (endtime, type) values (%s %s)'
    # insert_args = end_time, type_of_candle
    # cursor.execute(sql_insert, insert_args)
    sql_insert = f'insert into public.customcandle_lasttime (endtime, type) values ({end_time}, \'{type_of_candle}\')'
    cursor.execute(sql_insert)
    connection.commit()

    cursor.close()
    connection.close()
# end function



def generateCandles():
    for etf in etf_list:
        createCandles(etf)

def main():
    generateCandles()  

if __name__ == "__main__":
    main()