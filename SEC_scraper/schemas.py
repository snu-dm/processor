import datetime
from sqlalchemy import (
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    DateTime,
    SmallInteger,
    ForeignKey,
    Index,
    ARRAY,
    Boolean,
)
from sqlalchemy.sql.schema import MetaData

metadata = MetaData()

sec = Table(
    'sec', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('organization', String),
    Column('ticker', String, nullable=False),
    Column('year', Integer),
    Column('filingtype', String, nullable=False),
    Column('itemtype', String, nullable=False),
    Column('documentdate', DateTime),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('lastmodified', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
)