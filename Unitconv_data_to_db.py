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

dtype_std = {'unit_in': sqlalchemy.types.String,
             'unit_out': sqlalchemy.types.String,
             'factor': sqlalchemy.types.Numeric,
             }

tables = {'Unit_conversion': ['Unit_Conv.csv', dtype_std, 2018],
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
