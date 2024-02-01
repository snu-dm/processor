# This file uploads data to SNU MinIO Server
# Below codes borrow heavily from: https://min.io/docs/minio/linux/developers/python/minio-py.html

from minio import Minio
from minio.error import S3Error
import pyarrow as pa
import pyarrow.parquet as pq
import config
from sqlalchemy import create_engine, select, delete, insert, update
from schemas import sec_10k1, sec_10k7, sec_8k
import os
import argparse
from tqdm import tqdm

# Define arguments
parser = argparse.ArgumentParser(description='MinIO Data Uploader')
parser.add_argument('-b', '--bucket_name', type=str, default='corporate-finance',
                    help='Bucket name')
parser.add_argument('-ft', '--filing_type', type=str, required=True,
                    help='(REQUIRED) filing type to be uploaded; sec_10k1 or sec_10k7 or sec_8k'),
parser.add_argument('-oe', '--object_extension', type=str, default='parquet',
                    help='object type to be saved')
parser.add_argument('-fp', '--folder_path', type=str, required=True,
                    help='Folder directory with files to be uploaded')
args = parser.parse_args()
bucket_name = args.bucket_name
filing_type = args.filing_type
extension = args.object_extension
folder_path = args.folder_path

# Get Minio Access Info
ACCESS_KEY = os.environ.get("MINIO_USER","")
SECRET_KEY = os.environ.get("MINIO_SECRETKEY","")

#Create SQL Engine
engine = create_engine(f'postgresql://{config.user}:{config.pw}@{config.host}:{config.port}/{config.db}')
client = Minio(config.minio_api_endpoint, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

# Define SQL Queries
def define_queries(filing_type, ticker, path, company_name, cik, year, item_type, extension):
    if filing_type == 'sec_10k1': item_type = '10-K Item 1. Business'
    elif filing_type == 'sec_10k7': item_type = '10-K Item 7. Management Discussion and Analysis'
    else: item_type == '8-K'

    queries = insert(filing_type).values(
        path = path,
        ticker = ticker,
        company_name = company_name,
        cik = cik,
        year = year,
        item_type = item_type,
        extension = extension
    )
    return queries

# File Converter
def convert_to_parquet(text_file):
    

def main():
    print('Initiating the upload...')
    with engine.connect() as con:
        transactions = con.begin()
        for item in tqdm(os.listdir(folder_path)):
            ticker = item.split('.')[0]
            try:
                minio_object_name = 'SEC/f{filing_type}/f{year}/f{ticker}.{extension}'
                object_path='{}/{}'.format(bucket_name, minio_object_name)
                
                # PostgreSQL
                queries = define_queries(filing_type, ticker, path, company_name, cik, year, item_type, extension)
                con.execute(queries)

                # S3 (MINIO)
                df = pd.DataFrame([(filing_type, ticker, path, company_name, cik, year, item_type, extension)], \
                                    columns=['filing_type', 'ticker', 'path', 'company_name', 'cik', 'year', 'item_type', 'extension'])
                if not os.path.exists(os.path.dirname(object_path)):
                    os.makedirs(os.path.dirname(object_path))
                df.to_parquet(object_path)
                client.fput_object(
                    bucket_name, minio_object_name, object_path,
                )
                transactions.commit()
            except:
                print('Failed to INSERT data: {}'.format(minute_url))
                transactions.rollback()

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("S3Error! ", exc)
