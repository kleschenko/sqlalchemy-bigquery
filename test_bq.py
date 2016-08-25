'''
Created on 17 Aug 2016
'''

from sqlalchemy.dialects import registry
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, ForeignKey, select, String


registry.register("pandas", "calchipan.base", "PandasDialect")
registry.register("pandas.calchipan", "calchipan.base", "PandasDialect")

registry.register("bigquery", "sqlalchemy_bigquery.base", "BQDialect")
registry.register("bigquery.bq1", "sqlalchemy_bigquery.base", "BQDialect")


usePandas = False

if usePandas:
    engine = create_engine('pandas://')

else:
    engine = create_engine('bigquery://',
                           connect_args={'project': 'fastly-dw',
                                         'dataset': 'eu_datapath',
                                         'jsonAuthFile': '/Users/rlanda/Workspace/Python/datapath/auth/fastly_dw_auth.json'},
                           echo='debug')

engine.echo = True


metadata = MetaData(engine)

users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('age', Integer),
    Column('fullname', String),
)
users.create()


#connection = engine.connect()





#connection.close()

# from sqlalchemy_bigquery.dbapi import Connection, Cursor
# 
# con = Connection('fastly-dw',
#                  'eu_datapath',
#                  '/Users/rlanda/Workspace/Python/datapath/auth/fastly_dw_auth.json')
# 
# cur = con.cursor()
# cur.execute('select * from eu_datapath.`deepfield*` limit 123456', legacy_sql=False)
# 
# print(len(cur.fetchall()))


