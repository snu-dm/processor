import config
from sec_utils import *
from uploader import *
from sec_crawl import *
from parser2 import *
from schemas import sec

import pandas as pd

df = pd.read_csv("sp500")

for index, row in df.iterrows():
    ticker = str(row['ticker'])
    cik = str('{num:010d}'.format(num=row['cik']))

    dataList = crawl(ticker, cik)
    data_to_upload, ticker, year, filing_type, item_type, documentdate = parser(dataList)
    upload(data_to_upload, ticker, year, filing_type, item_type, documentdate)
    print(f"{ticker} 적재완료!!")
    print("다음 기업 데이터 적재를 시작합니다...")
