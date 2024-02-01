import datetime
from sqlalchemy import (
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    Date,
    DateTime,
    SmallInteger,
    ForeignKey,
    Index,
    ARRAY,
    Boolean,
)
from sqlalchemy.sql.schema import MetaData

metadata = MetaData()


# DART
dart = Table(
    'dart', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('path', String, nullable=False),
    Column('organization', String, nullable=False),
    Column('name', String, nullable=False),
    Column('year', Integer),
    Column('quarter', Integer), # 1, 2, 3, 4
    Column('correction', Integer), # 0 (정정 x), 1 (정정)
    Column('documentdate', Date),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('lastmodified', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
)


