import pandas as pd
from sqlalchemy import create_engine, select, delete, insert, update

import config
from schemas import stocks, minutes, metadata

#Create Engine
engine = create_engine(f'postgresql://{config.user}:{config.pw}@{config.host}:{config.port}/{config.db}')

#Make connection
con = engine.connect()

# ------------------------------------------------------------------------------------------------------------------- #
#                                                    Creating Table                                                   #
# ------------------------------------------------------------------------------------------------------------------- #
#metadata.create_all(bind=engine)


# ------------------------------------------------------------------------------------------------------------------- #
#                                              Inserting a record example                                             #
# ------------------------------------------------------------------------------------------------------------------- #
transactions = con.begin()

insert_query = insert(stocks).values(
    path='abc/efg/Apple.parquet',
    name='Apple Inc.',
    code=123,
    year=2023,
    quarter=1,
    itemtype='mytype',
    itemindex=123,
    extension='iMac'
)

con.execute(insert_query)
transactions.commit()

# session.rollback()
# if you want to rollback

# Recommendation: use with try-except
# transactions = con.begin()
# try: 
#   some code i want to run
#   transactions.commit()
# except:
#   transactions.rollback()

# ------------------------------------------------------------------------------------------------------------------- #
#                                             Retrieving a record example                                             #
# ------------------------------------------------------------------------------------------------------------------- #
query = select(stocks).where(stocks.c.name=='Apple Inc.')

#Get result in list of dict
result = con.execute(query).fetchall()

#Convert result to df
pd.DataFrame.from_records(result, columns=stocks.columns.keys())

#Or just use pandas
pd.read_sql(query, con)

# ------------------------------------------------------------------------------------------------------------------- #
#                                              Updating a record example                                              #
# ------------------------------------------------------------------------------------------------------------------- #
query = update(stocks).where(stocks.c.stockid == 1).values(name='Not Apple Inc.')
con.execute(query)

# Recommendation: use with try-except
# transactions = con.begin()
# try: 
#   some code i want to run
#   transactions.commit()
# except:
#   transactions.rollback()

# ------------------------------------------------------------------------------------------------------------------- #
#                                              Deleting a record example                                              #
# ------------------------------------------------------------------------------------------------------------------- #
query = delete(stocks).where(stocks.c.stockid == 1)
con.execute(query)

# Recommendation: use with try-except
# transactions = con.begin()
# try: 
#   some code i want to run
#   transactions.commit()
# except:
#   transactions.rollback()