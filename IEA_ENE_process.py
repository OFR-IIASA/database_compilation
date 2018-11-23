import sqlalchemy
import pandas as pd
import sys
from database_connect import database_connect


def inverse(mapping):
    """Returns and inverted dicitonary
    """
    new_dic = {}
    for k, v in mapping.items():
        for x in v:
            if x in new_dic:
                print(x, 'already exists in dictionary', new_dic)
                sys.exit(1)
            new_dic.setdefault(x, k)  # []).append(k)
    return(new_dic)


engine = database_connect()

# Retrieve raw data
# Filter out any data where:
# - COUNTRY is NULL
# - PROD_CODE is NULL
# - FLOW_CODE is NULL
sql = """SELECT country, product, flow, time, value FROM public."IEA_ENE_data_raw"
         WHERE country is not NULL and
               product is not NULL and
               flow is not NULL and
               value!=0;"""
df = pd.read_sql(sql, engine)

# Add FLOW_CAT (Categorization of flow codes based on
# World Energy Statistics:Database Documentaiton (2018 edition)
# Note that oil_demand has been omitted
FLOW_CAT = {
    'supply': ['INDPROD', 'OSCOAL', 'OSNATGAS',
               'OSOIL', 'OSRENEW', 'OSNONSPEC',
               'IMPORTS', 'EXPORTS', 'MARBUNK',
               'AVBUNK', 'STOCKCHA',  'DOMSUP',
               'TRANSFER', 'STATDIFF'],
    'transformation_processes': ['TOTTRANF', 'MAINELEC', 'AUTOELEC',
                                 'MAINCHP', 'AUTOCHP', 'MAINHEAT',
                                 'AUTOHEAT', 'THEAT', 'TBOILER',
                                 'TELE', 'TBLASTFUR', 'TGASWKS',
                                 'TCOKEOVS', 'TPATFUEL', 'TBKB',
                                 'TREFINER', 'TPETCHEM', 'TCOALLIQ',
                                 'TGTL', 'TBLENDGAS', 'TCHARCOAL',
                                 'TNONSPEC'],
    'energy_industry_own_use_and_losses': ['TOTENGY', 'EMINES', 'EOILGASEX',
                                           'EBLASTFUR', 'EGASWKS', 'EBIOGAS',
                                           'ECOKEOVS', 'EPATFUEL', 'EBKB',
                                           'EREFINER', 'ECOALLIQ', 'ELNG',
                                           'EGTL', 'EPOWERPLT', 'EPUMPST',
                                           'ENUC', 'ECHARCOAL', 'ENONSPEC',
                                           'DISTLOSS'],
    'final_consumption': ['FINCONS', 'TOTIND', 'IRONSTL',
                          'CHEMICAL', 'NONFERR', 'NONMET',
                          'TRANSEQ', 'MACHINE', 'MINING',
                          'FOODPRO', 'PAPERPRO', 'WOODPRO',
                          'CONSTRUC', 'TEXTILES', 'INONSPEC',
                          'TOTTRANS', 'WORLDAV', 'DOMESAIR',
                          'ROAD', 'RAIL', 'PIPELINE',
                          'WORLDMAR', 'DOMESNAV', 'TRNONSPE',
                          'TOTOTHER', 'RESIDENT', 'COMMPUB',
                          'AGRICULT', 'FISHING', 'ONONSPEC',
                          'NONENUSE', 'NEINTREN', 'NETRANS',
                          'NEOTHER', 'NEIND', 'NEIRONSTL',
                          'NECHEM', 'NENONFERR', 'NENONMET',
                          'NETRANSEQ', 'NEMACHINE', 'NEMINING',
                          'NEFOODPRO', 'NEPAPERPRO', 'NEWOODPRO',
                          'NECONSTRUC', 'NETEXTILES', 'NEINONSPEC'],
    'electricity_output': ['ELOUTPUT', 'ELMAINE', 'ELAUTOE',
                           'ELMAINC', 'ELAUTOC', 'MHYDPUMP',
                           'AHYDPUMP'],
    'heat_output': ['HEATOUT', 'HEMAINC', 'HEAUTOC',
                    'HEMAINH', 'HEAUTOH'],
}
df['flow_category'] = df['flow'].map(inverse(FLOW_CAT))

# Add PRDO_CAT (Categorization of flow codes based on
# World Energy Statistics:Database Documentaiton (2018 edition)
# Note that oil_demand has been omitted
PROD_CAT = {
    'coal': ['HARDCOAL', 'BROWN', 'ANTCOAL',
             'COKCOAL', 'BITCOAL', 'SUBCOAL',
             'LIGNITE', 'PATFUEL', 'OVENCOKE',
             'GASCOKE', 'COALTAR', 'BKB',
             'GASWKSGS', 'COKEOVGS', 'BLFURGS',
             'OGASES'],
    'peat_and_peat_products': ['PEAT', 'PEATPROD'],
    'oil_shale': ['OILSHALE'],
    'natural_gas': ['NATGAS'],
    'crude_ngl_refinery_feedstocks': ['CRNGFEED', 'CRUDEOIL', 'NGL',
                                      'REFFEEDS', 'ADDITIVE', 'NONCRUDE'],
    'oil_products': ['REFINGAS', 'ETHANE', 'LPG',
                     'NONBIOGASO', 'AVGAS', 'JETGAS',
                     'NONBIOJETK', 'OTHKERO', 'NONBIODIES',
                     'RESFUEL', 'NAPHTHA', 'WHITESP',
                     'LUBRIC', 'BITUMEN', 'PARWAX',
                     'PETCOKE', 'ONONSPEC'],
    'biofuels_and_waste': ['INDWASTE', 'MUNWASTER', 'MUNWASTEN',
                           'PRIMSBIO', 'BIOGASES', 'BIOGASOL',
                           'BIODIESEL', 'BIOJETKERO', 'OBIOLIQ',
                           'RENEWNS', 'CHARCOAL'],
    'electricity_and_heat': ['MANGAS', 'HEATNS', 'NUCLEAR',
                             'HYDRO', 'GEOTHERM', 'SOLARPV',
                             'SOLARTH', 'TIDE', 'WIND',
                             'HEATPUMP', 'BOILER', 'CHEMHEAT',
                             'OTHER', 'ELECTR', 'HEAT'],
}
df['product_category'] = df['product'].map(inverse(PROD_CAT))

# ADD Units, based on product, categories
# Note that products_for_oil_demand has been omitted
FLOW_CAT_UNIT = {
    'electricity_output': {'unit': 'GWh'},
    'heat_output': {'unit': 'TJ'},
}

PROD_CAT_UNIT = {
    'coal': {'unit': 'ktoe', 'exception': {'GASCOKE': 'TJ',
                                           'GASWKSGS': 'TJ',
                                           'COKEVGS': 'TJ'}},
    'peat_and_peat_products': {'unit': 'TJ'},
    'oil_shale': {'unit': 'ktoe'},
    'natural_gas': {'unit': 'TJ'},
    'crude_ngl_refinery_feedstocks': {'unit': 'ktoe'},
    'oil_products': {'unit': 'ktoe'},
    'biofuels_and_waste': {'unit': 'TJ', 'exception': {'BIODIESEL': 'kt',
                                                       'BIOJETKERO': 'kt',
                                                       'OBIOLIQ': 'kt',
                                                       'CHARCOAL': 'kt'}},
    'electricity_and_heat': {'unit': 'TJ'},
}


def add_unit(row, cat):
    if row['flow_category'] in FLOW_CAT_UNIT:
        return(FLOW_CAT_UNIT[row['flow_category']]['unit'])
    else:
        if row['product_category'] in PROD_CAT_UNIT:
            if 'exception' in PROD_CAT_UNIT[row['product_category']] and\
                    row['product'] in PROD_CAT_UNIT[
                        row['product_category']]['exception']:
                return(PROD_CAT_UNIT[
                    row['product_category']]['exception'][row['product']])
            else:
                return(PROD_CAT_UNIT[
                    row['product_category']]['unit'])


df['unit'] = df.apply(lambda row: add_unit(row, 'flow'), axis=1)

# Check if there are any un-allocated entries
if True in df.isnull().any().values:
    print('NaN contained in data; please revise')
    sys.exit(1)

dtype = {'country': sqlalchemy.types.String,
         'product': sqlalchemy.types.String,
         'flow': sqlalchemy.types.String,
         'time': sqlalchemy.types.Integer,
         'value': sqlalchemy.types.Numeric,
         'flow_category': sqlalchemy.types.String,
         'product_category': sqlalchemy.types.String,
         'unit': sqlalchemy.types.String,
         }

df.to_sql('IEA_ENE_data',
          engine,
          index=False,
          dtype=dtype)
