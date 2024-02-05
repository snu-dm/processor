import config
from sec_utils import *
from uploader import *
from sec_crawl import *
from parser import *
from schemas import sec

import pandas as pd
import re

df = pd.read_csv("sp500")

def extract_year_from_filename(filename):
    # 정규 표현식을 사용하여 'YYYYMMDD' 또는 'YYYY' 형식의 날짜 부분을 찾음
    match = re.search(r'(\d{4})', filename)
    
    if match:
        # 찾은 날짜 부분 반환
        return match.group()
    else:
        # 일치하는 날짜 부분이 없으면 None 반환
        return None

for index, row in df.iterrows():
    ticker = str(row['ticker'])
    cik = str('{num:010d}'.format(num=row['cik']))

    filing_type = "10-k"
    item_type = "item 1"


    dataList = crawl(ticker, cik)

    for data in dataList:
        data_to_upload, file_name, documentdate = parser(data)
        year = extract_year_from_filename(file_name)
        upload(data_to_upload, ticker, year, filing_type, item_type, documentdate)
        
    print(f"{ticker} 적재완료!!")
    print("다음 기업 데이터 적재를 시작합니다...")
