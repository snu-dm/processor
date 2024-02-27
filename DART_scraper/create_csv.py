import numpy as np
import pandas as pd
import requests
from urllib.request import urlopen
import urllib.parse
from zipfile import ZipFile
from io import BytesIO
import xml.etree.ElementTree as et

MARKET_CODE_DICT = {
    'kospi': 'stockMkt',
    'kosdaq': 'kosdaqMkt',
    'konex': 'konexMkt'
}

DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'

api_key = 'a7b222155e0ad5b1ed9de6838174eb585a7db8c9'
request_url = 'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key='

current_path = '/home/5yoondori/bdai_processor/processor/DART_scraper/'

# DART에서 stock codes 및 company names 다운로드
def download_stock_codes(market=None, delisted=False):
    params = {'method': 'download'}

    if market.lower() in MARKET_CODE_DICT:
        params['marketType'] = MARKET_CODE_DICT[market]

    if not delisted:
        params['searchType'] = 13

    params_string = urllib.parse.urlencode(params)
    download_url = urllib.parse.urlunsplit(['http', DOWNLOAD_URL, '', params_string, ''])
    print(f"download_url: {download_url}")

    df = pd.read_html(download_url, header=0, encoding='euc-kr')[0]
    df.종목코드 = df.종목코드.map('{:06d}'.format)

    return df

# 코스피, 코스닥 데이터 다운로드
kospi_stocks = download_stock_codes('kospi')
kospi_stocks['시장구분'] = 'KOSPI'
kospi_stocks = kospi_stocks[['회사명','종목코드']]

kosdaq_stocks = download_stock_codes('kosdaq')
kosdaq_stocks['시장구분'] = 'KOSDAQ'
kosdaq_stocks = kosdaq_stocks[['회사명','종목코드']]

# 코스피, 코스닥 데이터 concat 하여 하나의 df로 만들기
corp_info = pd.concat([kospi_stocks, kosdaq_stocks]).reset_index(drop=True)
corp_info = corp_info.rename(columns={'회사명':'종목명'})
corp_info.to_csv('corporation_information_2020.csv', index=False)

# dart에서 가능한 기업들 목록 다 가져오기
r = urlopen(request_url+api_key)

with ZipFile(BytesIO(r.read())) as zf:
    file_list = zf.namelist()
    while len(file_list) > 0:
        file_name = file_list.pop()
        corpCode = zf.open(file_name).read().decode()

tree = et.fromstring(corpCode)
stocklist = tree.findall('list')

corp_codes = [item.findtext("corp_code") for item in stocklist]
corp_names = [item.findtext("corp_name") for item in stocklist]
stock_codes = [item.findtext("stock_code") for item in stocklist]
modify_dates = [item.findtext("modify_date") for item in stocklist]

# 우리가 가진 코스피, 코스닥 기업들에 대한 자료들만 추려내자
wanted_stocks = corp_info['종목코드'].tolist()

corp_codes = [corp_codes[i] for i in range(len(corp_codes)) if stock_codes[i] in wanted_stocks]
corp_names = [corp_names[i] for i in range(len(corp_names)) if stock_codes[i] in wanted_stocks]
modify_dates = [modify_dates[i] for i in range(len(modify_dates)) if stock_codes[i] in wanted_stocks]
stock_codes = [stock_codes[i] for i in range(len(stock_codes)) if stock_codes[i] in wanted_stocks]

df = pd.DataFrame()
df['종목코드'] = stock_codes
df['종목명'] = corp_names
df['고유번호'] = corp_codes
df['최근변경일자'] = modify_dates

df.to_csv('DART_test.csv', encoding='utf8', index=False)