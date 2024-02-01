from sec_utils import *
import pandas as pd
import argparse

def crawl(ticker, cik):
    seccrawler = SecCrawler()
    # seccrawler.filing_10K('MSFT','0000789019','20210719','2000000')
    dataList = seccrawler.filing_10K(ticker, cik,'20240127', '100')

    print(f"num: {len(dataList)}")

    return dataList

#if __name__=='__crawl__':
#    parser = argparse.ArgumentParser()
#    parser.add_argument('ticker', type=str)
#    parser.add_argument('cik', type=str)
#    args = parser.parse_args()

#    crawl(args)