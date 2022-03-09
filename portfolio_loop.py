import pandas as pd
import numpy as np
import os

from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns

import pulp
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices

import time
import sys
import cvxpy as cp

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

path_project = os.path.join(os.getcwd(),'Prices')
# path_prices = os.path.join(path_project,'ETF')
# path_prices = os.path.join(path_project,'Stocks_yahoo')

df_stocks_list = pd.read_csv(os.path.join(path_project,'currencycom_info.csv'), sep=';')
df_stocks_list = df_stocks_list[(df_stocks_list['assetType']=='EQUITY')]
df_stocks_list['symbol'] = df_stocks_list['symbol'].str.split('.', n=1, expand=True)
df_stocks_list['symbol'] = df_stocks_list['symbol'].str.replace('[a-z]', '', regex=True)
# df_stocks_list['symbol'] = df_stocks_list['symbol'].str.replace('/', '-', regex=True)
# df_stocks_list = df_stocks_list[(df_stocks_list['marketType']=='SPOT') & (~(df_stocks_list['country']==''))]
df_stocks_list = df_stocks_list.drop_duplicates(subset=['symbol'])

df_sector = df_stocks_list

portfolio_val = 10000

files_stocks = 'all_stocks.csv'
df = pd.read_csv(files_stocks, sep=';', decimal=',')
df['Date'] = pd.to_datetime(df['Date'])
df.set_index('Date', inplace=True)

# import datetime as DT

# start_date = DT.datetime(2022, 2, 1)
# end_date = DT.datetime(2022, 3, 1)

# high_market = pd.date_range(
#     min(start_date, end_date),
#     max(start_date, end_date), freq="M"
# ).strftime('%Y-%m-%d').tolist()
high_market = ['2022-02-24']

def calc_ef(df_time, list_return):
	global dfs, dfe
	try:

		mu = expected_returns.mean_historical_return(df_time)
		S = risk_models.sample_cov(df_time)
		ef = EfficientFrontier(mu, S)

		weight = ef.max_sharpe()
		cleaned_weight = ef.clean_weights()
		ear_v_sharpe = ef.portfolio_performance()
		latest_prices = get_latest_prices(df_time)
		da = DiscreteAllocation(cleaned_weight, latest_prices, total_portfolio_value=portfolio_val)
		allocation, leftover = da.lp_portfolio(solver=cp.ECOS_BB)

	except ValueError as e:
		print(e)
		return 0

	if 'ear_v_sharpe' in locals():
		list_return.extend([round(elem, 2) for elem in ear_v_sharpe])
		dfe.append(list_return)

		df_portfolio = pd.DataFrame(allocation.items(), columns=['Symbol', 'Quantity']).set_index('Symbol')
		df_last_price = latest_prices.to_frame(name='Price')
		df_pay = pd.merge(df_portfolio, df_last_price, how="left", left_index=True, right_index=True)
		df_pay['Date'] = df_time.index.max().strftime('%d.%m.%Y')
		df_pay['Sector'] = list_return[1]
		df_pay = df_pay.reset_index()
		dfs.append(df_pay[['Symbol', 'Sector', 'Date', 'Price', 'Quantity']])


for cut_date in high_market:

	dfs = []
	dfe = []
	for sector in df_sector['sector'].unique():
		if (sector is np.NaN) \
			| ((cut_date=='2021-10-31') & (sector == 'Utilities')):
			continue
		list_return = [cut_date, sector]
		print(list_return)

		list_symbols_sector = df_sector[df_sector['sector'] == sector]['symbol'].to_list()
		list_symbols = [col for col in df.columns if col in list_symbols_sector]
		df_time = df[:cut_date][list_symbols]

		df2 = df_time.iloc[-1]
		df_symbol_nan = df2[df2.isnull()==True]
		list_symbol_nan = df_symbol_nan.reset_index()['index'].to_list()
		df_time = df_time[df_time.columns.difference(list_symbol_nan)]

		try:
			calc_ef(df_time, list_return)
		except ValueError as e:
			print(e)
			continue
	
	dfs_result = pd.concat(dfs)
	dfe_result = pd.DataFrame(dfe, columns =['Date', 'sector', 'ear', 'volatility', 'sharpe']) 
	files_evs = r'evs.csv'
	files_stocks = r'stocks.csv'
	dfs_result.to_csv(files_stocks,  mode='a', header=False, sep=';', decimal=',', index=False)
	dfe_result.to_csv(files_evs, mode='a', header=False, sep=';', decimal=',', index=True)