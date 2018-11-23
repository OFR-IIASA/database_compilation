import sqlalchemy
import pandas as pd
import os
from database_connect import database_connect
from load_config import load_config

# Set option for importing sql data
# Options are 'fail', 'replace', 'append', default 'fail'
db_write = 'replace'

# Set path where data is located
data_path = load_config()['data_path']

engine = database_connect()

dtype_std = {'name': sqlalchemy.types.String,
             'chemical formula': sqlalchemy.types.String,
             'Lifetime': sqlalchemy.types.Integer,
             'Radiative Efficiency': sqlalchemy.types.Numeric,
             '100-yr SAR': sqlalchemy.types.Numeric,
             '20-yr': sqlalchemy.types.Numeric,
             '100-yr': sqlalchemy.types.Numeric,
             '500-yr': sqlalchemy.types.Numeric,
             'Comment': sqlalchemy.types.String,
             }

tables = {'Global_warming_potential': ['GWP1.csv', dtype_std, 2007],
          }

for table in tables:
    df = pd.read_csv(os.path.join(data_path, tables[table][0]))
    df.columns = [c.lower() for c in df.columns]
    df['version'] = tables[table][2]
    df.to_sql(table,
              engine,
              if_exists=db_write,
              index=False,
              dtype=tables[table][1])
