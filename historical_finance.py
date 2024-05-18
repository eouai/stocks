import os
import json
import time
import tqdm
import random
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup as bs
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

initialize_VPN(save=1, area_input=['United States'])
URL = 'https://finance.yahoo.com/quote/'
TIMESTAMP = datetime.datetime.today().strftime('%Y-%m-%d')
agents = ['Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
			'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
			'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
			'Mozilla/5.0 (CrKey armv7l 1.5.16041) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.0 Safari/537.36',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80',
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80',
			'Mozilla/5.0 (Linux; Android 10; HD1913) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.113 Mobile Safari/537.36 EdgA/124.0.2478.62',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',]
			

path = os.path.normpath('C:\\Users\\Burt\\Documents\\Git\\podcast\\')
history_columns = ['ticker_yahoo', 'date', 'open', 'high', 'low', 'close', 'close_adj', 'volume', 'dividend', 'TIMESTAMP']
history_df = pd.DataFrame(columns=history_columns)
vpn_count = 0

with open(os.path.join(path, 'stocks_errors.json'), 'r') as f:
	errors = json.loads(f.read())
df = pd.read_excel(os.path.join(path, 'stocks_meta_master.xlsx'))
start = list(df['ticker_yahoo'])
df_done = pd.read_excel(os.path.join(path, 'stocks_history_master.xlsx'))
done = list(set(df_done['ticker_yahoo']))
tickers = [ x for x in start if x not in done ]

def parse_json_recursively(json_object, target_key, search_res=[]):
	if type(json_object) is dict and json_object:
		for key in json_object:
			if key == target_key:
				search_res.append(json_object[key])
			parse_json_recursively(json_object[key], target_key, search_res)
	elif type(json_object) is list and json_object:
		for item in json_object:
			parse_json_recursively(item, target_key, search_res)
	return search_res

for ticker in tqdm.tqdm(tickers[0:1000]):
	try:
		time.sleep(1)
		headers = {'User-Agent': agents[random.randrange(0,len(agents)-1)]}
		stock_url = os.path.join(URL, ticker + '/history')
		print('stock url:', stock_url)
		stock_res = requests.get(stock_url, headers=headers)
		# print('status code', stock_res.status_code)
		content = stock_res.content.decode()
		# print('content loaded')
		soup = bs(content, 'lxml')
		tables = soup.findAll('table')
		for table in tables:
			body = table.find('tbody')
			tr_s = body.findAll('tr')
			for tr in tr_s:
				date_, open_, high_, low_, close_, close_adj, volume, dividend = '','','','','','','',''
				dividend_data, trade_data = [],[]
				td_s = tr.findAll('td')
				if 'Splits' in tr.text or 'Capital' in tr.text:
					continue
				elif 'Dividend' in tr.text:
					for td in td_s:
						dividend_data.append(td.text)
					date, dividend = dividend_data[0], dividend_data[1]
				else:
					for td in td_s:
						trade_data.append(td.text)
					date_, open_, high_, low_, close_, close_adj, volume = trade_data[0],trade_data[1],trade_data[2],trade_data[3],trade_data[4],trade_data[5],trade_data[6],
				history_df.loc[len(history_df.index)] = [ticker, date_, open_, high_, low_, close_, close_adj, volume, dividend, TIMESTAMP]

		history_df.to_excel(os.path.join(path, 'stocks_history.xlsx'), index=False)
		print(ticker, date_, open_, high_, low_, close_, close_adj, volume, dividend, TIMESTAMP)
		
		vpn_count += 1
		if vpn_count > 30:
			time.sleep(10)
			vpn_count = 0
			rotate_VPN()
		
	except Exception as e:
		time.sleep(10)
		if errors.get(URL) is None:
			errors[URL] = {'count': 1, str(e): 1}
		else:
			errors[URL]['count'] += 1
			if errors.get(URL).get(str(e)) is None:
				errors[URL][str(e)] = 1
			else:
				errors[URL][str(e)] += 1
		with open(os.path.join(path, 'stocks_errors.json'), 'w') as f:
			json.dump(errors, f)
		print(str(e))
