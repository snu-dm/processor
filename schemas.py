import datetime
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.sql.schema import MetaData

# Declare metadata instance
metadata = MetaData()

sec_10k1 = Table(
    'sec_10k1', metadata,
    Column('ticker', String, primary_key=True, autoincrement=True),
    Column('path', String, nullable=False),
    Column('company_name', String, nullable=False),
    Column('cik', String),
    Column('year', Integer),
    Column('item_type', String),
    Column('extension',String),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('lastmodified', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
)

sec_10k7 = Table(
    'sec_10k7', metadata,
    Column('ticker', String, primary_key=True, autoincrement=True),
    Column('path', String, nullable=False),
    Column('company_name', String, nullable=False),
    Column('cik', String),
    Column('year', Integer),
    Column('item_type', String),
    Column('extension',String),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('lastmodified', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
)

sec_8k = Table(
    'sec_8k', metadata,
    Column('ticker', String, primary_key=True, autoincrement=True),
    Column('path', String, nullable=False),
    Column('company_name', String, nullable=False),
    Column('cik', String),
    Column('year', Integer),
    Column('quater', Integer),
    Column('item_type', String),
    Column('extension',String),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('lastmodified', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
)
