from datetime import datetime
import pandas as pd
import numpy as np
import time
from alpha_vantage.timeseries import TimeSeries
import os
from sqlalchemy import create_engine
from sqlalchemy.types import String

import execute_db

engine = create_engine('postgresql://postgres:admin@localhost:5432/alpha_vantage')


def AV_US(ts, tickers):
    
    df_aapl, meta_data = ts.get_intraday(symbol=tickers[0], interval='1min')
    df_qure, meta_data = ts.get_intraday(symbol=tickers[1], interval='1min')
    df_cldr, meta_data = ts.get_intraday(symbol=tickers[2], interval='1min')

    return df_aapl, df_qure, df_cldr

def AV_EUR(ts, tickers):
    
    df_evt,  meta_data = ts.get_intraday(symbol=tickers[0], interval='1min')
    df_argx, meta_data = ts.get_intraday(symbol=tickers[1], interval='1min')

    return df_evt, df_argx

def data_postprocess(data_dfs):
    
    df_vect = []
    for df_i in (data_dfs):
        # Rename columns
        df = df_i.rename(columns={"1. open": "open", 
                                "2. high": "high",
                                "3. low" : "low",
                                "4. close": "close",
                                "5. volume": "volume"})
        df = df.reset_index()
        df_snp = df[-10:]   # Sample only last 10 minutes of data
        
        df_vect.append(df_snp)
    
    return df_vect
    

# MAIN #
def run_main(API_key):
    cwd = os.getcwd()
    dir_data = cwd + r'\collected_data'
    # For Alpha-Vantage
    ts = TimeSeries(key=API_key, output_format='pandas')

    tickers_US = ['AAPL', 'QURE', 'CLDR']
    tickers_EUR = ['EVT.DE', 'ARGX.BR']

    tickers_db = ['aapl', 'qure', 'cldr', 'evt', 'argx']

    # Create DB
    list_DBs = execute_db.getDBs()
    db_name = 'alpha_vantage'
    if db_name not in list_DBs:
        execute_db.build_db(db_name)
        execute_db.build_table(db_name, tickers_db)


    while True:

        timestamp = datetime.now()

        if (timestamp.isoweekday() in range(1, 6)) & (timestamp.hour in range(15, 24)): # (15 ,22)
            if (timestamp.hour == 15) & (timestamp.minute > 30):
                # Get Alpha Vantage data
                df_aapl = AV_US(ts, tickers_US)
            elif (timestamp.hour > 15):
                # Get Alpha Vantage data
                df_aapl, df_qure, df_cldr = AV_US(ts, tickers_US)
                
            # Prepare data for db
            data_dfs = (df_aapl, df_qure, df_cldr)
            df_vect = data_postprocess(data_dfs)
            
            df_aapl_snp = df_vect[0]
            df_qure_snp = df_vect[1]
            df_cldr_snp = df_vect[2]
            
            # Save to db
            df_aapl_snp.to_sql('raw_aapl', engine,  if_exists='append', index=False)
            df_qure_snp.to_sql('raw_qure', engine,  if_exists='append', index=False)
            df_cldr_snp.to_sql('raw_cldr', engine,  if_exists='append', index=False)

        if (timestamp.isoweekday() in range(1, 6)) & (timestamp.hour in range(9, 24)): # (9, 18)
            if (timestamp.hour < 24): # (h = 17)
               # get alpha vantage data
                df_evt, df_argx = AV_EUR(ts, tickers_EUR)
            elif (timestamp.hour == 24) & (timestamp.minute < 30): #(h = 17)
                # get alpha vantage data
                df_evt, df_argx = AV_EUR(ts, tickers_EUR)
            
            # Prepare data for db
            data_dfs = (df_evt, df_argx)
            df_vect = data_postprocess(data_dfs)
            
            df_evt_snp = df_vect[0]
            df_argx_snp = df_vect[1]
            
            # Save to db
            df_evt_snp.to_sql('raw_evt', engine,  if_exists='append', index=False)
            df_argx_snp.to_sql('raw_argx', engine,  if_exists='append', index=False)
            
        else:
            print('(AV) Time out of range: weekday - % 2d, hour - % 2d' % (timestamp.isoweekday(), timestamp.hour))

        print('(AV) Waiting to refresh...')
        time.sleep(60*10)  # refresh every 10 min

if __name__ == '__main__':
    print('################# START #######################')
    
    API_key = 'O0S8HZHMVA0B6L52'
    
    run_main(API_key)

    