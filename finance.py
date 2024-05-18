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
metadata_columns = ['ticker_yahoo', 'ticker_google', 'name', 'Q2_2024_date', 'Q1_2024_date', 'Q4_2023_date', 'Q3_2023_date', 'Q2_2023_date', 'exchange', 'sector', 'market_cap', 
					'Q1_2024_eps_est', 'Q4_2023_eps_est', 'Q3_2023_eps_est', 'Q2_2023_eps_est', 
					'Q1_2024_eps_act', 'Q4_2023_eps_act', 'Q3_2023_eps_act', 'Q2_2023_eps_act',
					'TIMESTAMP']
metadata_df = pd.DataFrame(columns=metadata_columns)
vpn_count = 0

with open(os.path.join(path, 'movies_errors.json'), 'r') as f:
	errors = json.loads(f.read())
df = pd.read_excel(os.path.join(path, 'stocks.xlsx'))
start = list(df['ticker_yahoo'])
df_done = pd.read_excel(os.path.join(path, 'stocks_meta_master.xlsx'))
done = list(df_done['ticker_yahoo'])
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
		Q2_2024_date, Q1_2024_date, Q4_2023_date, Q3_2023_date, Q2_2023_date, exchange, sector, market_cap, name = '','','','','','','','',''
		Q1_2024_eps_est, Q4_2023_eps_est, Q3_2023_eps_est, Q2_2023_eps_est = None,None,None,None
		Q1_2024_eps_act, Q4_2023_eps_act, Q3_2023_eps_act, Q2_2023_eps_act = None,None,None,None
		ticker_google = ''
		time.sleep(1)
		headers = {'User-Agent': agents[random.randrange(0,len(agents)-1)]}
		stock_url = os.path.join(URL, ticker)
		# print('stock url:', stock_url)
		stock_res = requests.get(stock_url, headers=headers)
		# print('status code', stock_res.status_code)
		content = stock_res.content.decode()
		# print('content loaded')
		soup = bs(content, 'lxml')
		h1s = soup.findAll('h1')
		for h1 in h1s:
			if ticker in h1.text:
				name = h1.text
		h2s = soup.findAll('h2')
		for h2 in h2s:
			if h2.get('class') is not None and 'svelte-1xu2f9r' in h2.get('class'):
				sector = h2.text.split('Overview')[-1]
				# print('sector:', sector)
				break
		tags = soup.findAll('fin-streamer')
		for tag in tags:
			if 'svelte-tx3nkj' in tag.get('class') and 'marketCap' in tag.get('data-field'):
				market_cap = tag.text.strip()
				# print('market cap:', market_cap)
				break
		lis = soup.findAll('li')
		for li in lis:
			if li.get('class') is not None and 'svelte-tx3nkj' in li.get('class') and 'Earnings Date' in li.text:
				Q2_2024_date = li.text.split('Earnings Date')[-1].strip()
				# print('Q2 date:', Q2_2024_date)
				break
		spans = soup.findAll('span')
		for span in spans:
			if span.get('class') is not None and 'exchange' in span.get('class'):
				exchange = span.text.split('-')[0].strip()
				# exchange = span.text.split('•')[0].strip()
				# print('exchange:', exchange)
		additional_url = 'https://finance.yahoo.com/quote/{}/analysis'.format(ticker)
		stock_res = requests.get(additional_url, headers=headers)
		print('additional url:', additional_url)
		# print('status code:', stock_res.status_code)
		content = stock_res.content.decode()
		soup = bs(content, 'lxml')
		sections = soup.findAll('section')
		# print('found {} sections:'.format(len(sections)))
		for section in sections:
			if section.get('data-testid') is not None and 'earningsHistory' in section.get('data-testid'):
				EPS_headers = []
				EPS_estimate = []
				EPS_actual = []
				tr_s = section.findAll('tr')
				for tr in tr_s:
					if len(tr.findAll('th')) > 0:
						th_s = tr.findAll('th')
						for th in th_s:
							EPS_headers.append(th.text)
					elif 'EPS Est.' in tr.text:
						td_s = tr.findAll('td')
						for td in td_s:
							EPS_estimate.append(td.text)
					elif 'EPS Actual' in tr.text:
						td_s = tr.findAll('td')
						for td in td_s:
							EPS_actual.append(td.text)
					else:
						continue

				Q2_2023_date, Q3_2023_date, Q4_2023_date, Q1_2024_date = EPS_headers[1],EPS_headers[2],EPS_headers[3],EPS_headers[4]
				Q2_2023_eps_est, Q3_2023_eps_est, Q4_2023_eps_est, Q1_2024_eps_est  = EPS_estimate[1],EPS_estimate[2],EPS_estimate[3],EPS_estimate[4]
				Q2_2023_eps_act, Q3_2023_eps_act, Q4_2023_eps_act, Q1_2024_eps_act = EPS_actual[1],EPS_actual[2],EPS_actual[3],EPS_actual[4]
				
		metadata_df.loc[len(metadata_df.index)] = [ticker, ticker_google, name, Q2_2024_date, Q1_2024_date, Q4_2023_date, Q3_2023_date, Q2_2023_date, exchange, sector, market_cap,
													Q1_2024_eps_est, Q4_2023_eps_est, Q3_2023_eps_est, Q2_2023_eps_est,
													Q1_2024_eps_act, Q4_2023_eps_act, Q3_2023_eps_act, Q2_2023_eps_act,
													TIMESTAMP]
		metadata_df.to_excel(os.path.join(path, 'stocks_meta.xlsx'), index=False)
		print(Q2_2024_date, Q1_2024_date, Q4_2023_date, Q3_2023_date, Q2_2023_date, exchange, sector, market_cap)
		print(Q1_2024_eps_est, Q4_2023_eps_est, Q3_2023_eps_est, Q2_2023_eps_est)
		print(Q1_2024_eps_act, Q4_2023_eps_act, Q3_2023_eps_act, Q2_2023_eps_act)
		
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
		with open(os.path.join(path, 'movies_errors.json'), 'w') as f:
			json.dump(errors, f)
		print(str(e))
