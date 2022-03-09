import os
from datetime import datetime, timedelta
from concurrent import futures
import pandas as pd
# import pandas_datareader as pdr
import yfinance as yf


def get_exchange_info():
	import requests
	API_VERSION = 'v1'
	BASE_URL = 'https://api-adapter.backend.currency.com/api/{}/'.format(API_VERSION)
	EXCHANGE_INFORMATION_ENDPOINT = BASE_URL + 'exchangeInfo'
	r = requests.get(EXCHANGE_INFORMATION_ENDPOINT)
	return r.json()

def convert_dict_symbols_to_df(dict_symbols):
    df_symbols = pd.DataFrame(dict_symbols["symbols"])
    return df_symbols

# Изменить папку актива
folder = 'Stocks_yahoo'
bad_file = 'bad_yahoo_symbols.csv'

# Sorces
path_project = os.path.join(os.getcwd(),'Prices')
path_prices = os.path.join(path_project,folder)

path_bad_symbols = os.path.join(path_project,bad_file)  # to keep track of failed queries
df_bad_symbols = pd.read_csv(path_bad_symbols, sep=',')
df_bad_symbols = df_bad_symbols[df_bad_symbols['Exception'].str.startswith('No data fetched')]

# set the download window
now_time = datetime.now()
end_date = datetime(now_time.year, now_time.month, now_time.day) - timedelta(days=1)
list_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

def download_stock(symbol):
    global df_bad_symbols
    global end_date
    try:
        if symbol not in df_bad_symbols['bad_symbols'].values:
            path = os.path.join(path_prices,symbol+'.csv')
            is_file = os.path.isfile(path)
            if is_file:
                df_symbols = pd.read_csv(path, sep=',')
                start_date = datetime.strptime(df_symbols['Date'].max(), "%Y-%m-%d") + timedelta(days=1)
            else:
                start_date = datetime(1970, 1, 2)
                
            if start_date <= end_date:
                # df_stock = pdr.data.DataReader(symbol, 'yahoo', start_date, end_date)
                df_stock = yf.download(symbol, start=start_date, end=end_date, progress=False)
                if not df_stock.empty:
                    df_stock = df_stock.round(2)
                    df_stock['Volume'] = df_stock['Volume'].round(0)
                    df_stock.reset_index(inplace=True)
                    # df_stock.drop_duplicates(subset=['Date'], inplace=True)
                    df_stock[list_columns].to_csv(path, mode='a', header=False if is_file else True, index=False)
                    print(path)
                else:
                    df_bad_symbols.append((symbol,'No data fetched'), ignore_index=True)
            else:
                print("Loaded " + symbol)
    except Exception as e:
        dict_bad_symbol = dict(zip(df_bad_symbols.columns, [symbol,e]))
        print(dict_bad_symbol)
        df_bad_symbols = df_bad_symbols.append(dict_bad_symbol, ignore_index=True)

if __name__ == '__main__':

    df_com_symbols = convert_dict_symbols_to_df(get_exchange_info())
    df_com_symbols.to_csv(os.path.join(path_project,'currencycom_info.csv'), sep=';')
    df_com_symbols = df_com_symbols[(df_com_symbols['assetType']=='EQUITY')]
    df_com_symbols['symbol'] = df_com_symbols['symbol'].str.split('.', n=1, expand=True)
    df_com_symbols['symbol'] = df_com_symbols['symbol'].str.replace('[a-z]', '', regex=True)
    df_com_symbols = df_com_symbols.drop_duplicates(subset=['symbol'])
    df_symbols = df_com_symbols.set_index('symbol')

    list_symbols = tuple(df_symbols.index.to_list())
    # list_symbols = ('MGA', 'ANA') #, 'CTLT', 'SCR', 'ZM')

    """here we use the concurrent.futures module's ThreadPoolExecutor
        to speed up the downloads buy doing them in parallel 
        as opposed to sequentially """

    # set the maximum thread number
    workers = min(os.cpu_count(), len(list_symbols))    # in case a smaller number of stocks than threads was passed in
    with futures.ThreadPoolExecutor(workers) as executor:
        executor.map(download_stock, list_symbols)

    # for symbol in list_symbols:
    #     download_stock(symbol)
    # df_bad_symbols.to_csv(path_bad_symbols, index=False)

    # timing:
    finish_time = datetime.now()
    duration = finish_time - now_time
    minutes, seconds = divmod(duration.seconds, 60)
    print(f'The threaded script took {minutes} minutes and {seconds} seconds to run.')
