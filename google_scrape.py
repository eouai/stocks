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
URL = 'https://www.google.com/finance/quote/'
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
meta_columns = ['ticker_google', 'Q1_2024_reported', 'name', 'market_cap', 'TIMESTAMP']
meta_df = pd.DataFrame(columns=meta_columns)
vpn_count = 0

with open(os.path.join(path, 'stocks_errors.json'), 'r') as f:
	errors = json.loads(f.read())
df = pd.read_excel(os.path.join(path, 'stocks_meta_master.xlsx'))
start = list(df['ticker_google'])
df_done = pd.read_excel(os.path.join(path, 'stocks_google_master.xlsx'))
done = list(set(df_done['ticker_google']))
tickers = [ x for x in start if x not in done ]
print(tickers)

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
		# stock_url = os.path.join(URL, ticker)
		stock_url = URL + ticker
		print('stock url:', stock_url)
		stock_res = requests.get(stock_url, headers=headers)
		# print('status code', stock_res.status_code)
		content = stock_res.content.decode()
		# print('content loaded')
		soup = bs(content, 'lxml')
		divs = soup.findAll('div')
		Q1_2024_reported, name, market_cap = '','',''
		for div in divs:
			if div.get('class') is not None and 'EY8ABd-OWXEXe-TAWMXe' in div.get('class') and 'Reported' in div.text:
				Q1_2024_reported = div.text
			if div.get('class') is not None and 'zzDege' in div.get('class'):
				name = div.text
			if div.get('class') is not None and 'gyFHrc' in div.get('class') and 'Market cap' in div.text:
				subs = div.findAll('div')
				for sub in subs:
					if sub.get('class') is not None and 'P6K39c' in sub.get('class'):
						market_cap = sub.text
		
		meta_df.loc[len(meta_df.index)] = [ticker, Q1_2024_reported, name, market_cap, TIMESTAMP]

		meta_df.to_excel(os.path.join(path, 'stocks_google_data.xlsx'), index=False)
		print(ticker, Q1_2024_reported, name, market_cap, TIMESTAMP)
		
		vpn_count += 1
		if vpn_count > 20:
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
