import pandas as pd
from sqlalchemy import create_engine, select, delete, insert, update
import config
from schemas import sec
from minio import Minio
from minio.error import S3Error
import psycopg2
import io

# --------------------------------------------------------Database Setting--------------------------------------------------------
# Create Engine

engine = create_engine(f'postgresql://{config.user}:{config.pw}@{config.host}:{config.port}/{config.db}')
client = Minio(config.minio_api_endpoint, access_key=config.user, secret_key=config.user, secure=False)

def get_insert_query(ticker, year, filing_type, item_type, document_date):
    insert_query = insert(sec).values(
        organization = 'SEC',
        ticker = ticker,
        year = year,
        filingtype = filing_type,
        itemtype = item_type,
        documentdate = document_date
    )

    return insert_query

###########################################################################
#  DATABASE SETTING # END
###########################################################################

def upload(data_to_upload, ticker, year, filing_type, item_type, document_date, insert_into_NRFDB=True):
    if insert_into_NRFDB:
        with engine.connect() as con:

            transactions = con.begin()
            
            try:
                bucket_name = 'corporate-finance'
                minio_object_name = f'SEC/sp500/{ticker}/{filing_type}/{item_type}/{year}.txt'

                # PostgreSQL
                insert_query = get_insert_query(ticker, year, filing_type, item_type, document_date)
                con.execute(insert_query)

                #preprocessing for upload
                data_to_upload_bytes = data_to_upload.encode('utf-8')
                data_stream = io.BytesIO(data_to_upload_bytes)

                # S3 (MINIO)
                client.put_object(
                    bucket_name, minio_object_name, data_stream, len(data_to_upload_bytes)
                )
                transactions.commit()
                print(f"success to upload {ticker}-{year}!!!")

            except Exception as e:
                print('Failed to INSERT data: {}/{}'.format(ticker, year))
                print(f"e: {e}")
                transactions.rollback()
    

            
            