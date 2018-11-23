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

dtype_std = {'country': sqlalchemy.types.String,
             'product': sqlalchemy.types.String,
             'flow': sqlalchemy.types.String,
             'time': sqlalchemy.types.Integer,
             'value': sqlalchemy.types.Numeric,
             'flag codes': sqlalchemy.types.String,
             'version': sqlalchemy.types.Integer
             }
dtype_tax = {'iea_location': sqlalchemy.types.String,
             'iea_product': sqlalchemy.types.String,
             'sector': sqlalchemy.types.String,
             'iea_flow': sqlalchemy.types.String,
             'time': sqlalchemy.types.String,
             'value': sqlalchemy.types.Numeric,
             'flag codes': sqlalchemy.types.String,
             'version': sqlalchemy.types.Integer
             }
tables = {'IEA_CO2_data_raw': ['CO2-en.csv', dtype_std, 2018],
          'IEA_RENCAP_data_raw': ['REN_CAPACITIES-en.csv', dtype_std, 2008],
          'IEA_ENETax_data_raw': ['END_TOE-en.csv', dtype_tax, 2008],
          'IEA_ENE_data_raw': ['Full dataset.csv', dtype_std, 2018]
          }

for table in tables:
    df = pd.read_csv(os.path.join(data_path, tables[table][0]))
    df.columns = [c.lower() for c in df.columns]
    df['version'] = tables[table][2]
    if table == 'IEA_ENE_data_raw':
        cnt = 0
        for country in df['country'].unique().tolist():
            tmp = df[df['country'] == country]
            if cnt == 0:
                tmp.to_sql(table,
                           engine,
                           if_exists=db_write,
                           index=False,
                           dtype=tables[table][1])
                cnt += 1
            else:
                tmp.to_sql(table,
                           engine,
                           if_exists='append',
                           index=False,
                           dtype=tables[table][1])
    else:
        df.to_sql(table,
                  engine,
                  if_exists=db_write,
                  index=False,
                  dtype=tables[table][1])
