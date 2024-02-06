import pandas as pd
from sqlalchemy import create_engine, select, delete, insert, update

import config
from schemas import metadata

#Create Engine
engine = create_engine(f'postgresql://{config.user}:{config.pw}@{config.host}:{config.port}/{config.db}')

#Make connection
con = engine.connect()

# ------------------------------------------------------------------------------------------------------------------- #
#                                                    Creating Table                                                   #
# ------------------------------------------------------------------------------------------------------------------- #
metadata.create_all(bind=engine)