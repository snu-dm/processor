import pandas as pd
from sqlalchemy import create_engine, select, delete, insert, update

import config
from schemas import metadata, dart

#Create Engine
engine = create_engine(f'postgresql://{config.user}:{config.pw}@{config.host}:{config.port}/{config.db}')

#Make connection
con = engine.connect()

# ------------------------------------------------------------------------------------------------------------------- #
#                                                   Deleting all rows                                                 #
# ------------------------------------------------------------------------------------------------------------------- #
transactions = con.begin()

try: 
  query = dart.delete()
  con.execute(query)
  transactions.commit()
except:
  transactions.rollback()