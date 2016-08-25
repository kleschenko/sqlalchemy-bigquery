"""
Support for Google BigQuery.

"""
from sqlalchemy import sql, types, pool
from sqlalchemy.engine import default, reflection

from bqtypes import (BQString, BQInteger, BQFloat, BQTimestamp, BQBytes, BQBoolean)
from compiler import BQSQLCompiler, BQDDLCompiler

import dbapi


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQExecutionContext(default.DefaultExecutionContext):
    def get_lastrowid(self):
        return self.cursor.lastrowid


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQIdentifierPreparer(sql.compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(BQIdentifierPreparer, self).__init__(dialect)

    def format_label(self, label, name=None):
        """ bq can't handle quoting labels """
        return name or label.name


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQDialect(default.DefaultDialect):
    colspecs = {
        types.Unicode: BQString,
        types.Integer: BQInteger,
        types.SmallInteger: BQInteger,
        types.Numeric: BQFloat,
        types.Float: BQFloat,
        types.DateTime: BQTimestamp,
        types.Date: BQTimestamp,
        types.String: BQString,
        types.LargeBinary: BQBytes,
        types.Boolean: BQBoolean,
        types.Text: BQString,
        types.CHAR: BQString,
        types.TIMESTAMP: BQTimestamp,
        types.VARCHAR: BQString
    }

    __TYPE_MAPPINGS = {'TIMESTAMP': types.DateTime(),
                       'STRING': types.String(),
                       'FLOAT': types.Float(),
                       'INTEGER': types.Integer(),
                       'BOOLEAN': types.Boolean()}

    name = 'bigquery'
    driver = 'bq1'
    poolclass = pool.SingletonThreadPool
    statement_compiler = BQSQLCompiler
    ddl_compiler = BQDDLCompiler
    preparer = BQIdentifierPreparer
    execution_ctx_cls = BQExecutionContext

    supports_alter = False
    supports_unicode_statements = True
    supports_sane_multi_rowcount = False
    supports_sane_rowcount = False
    supports_sequences = False
    supports_native_enum = False

    positional = False
    paramstyle = 'named'

    default_sequence_base = 0
    default_schema_name = None

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def __init__(self, **kw):
        #
        # Create a dialect object
        #
        super(BQDialect, self).__init__(**kw)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def create_connect_args(self, url):
        #
        # This function recovers connection parameters from the connection string
        #
        return [], {}

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def initialize(self, connection):
        """disable all dialect initialization"""

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    @classmethod
    def dbapi(cls):
        return dbapi

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_schema_names(self, engine, **kw):
        return engine.connect().connection.get_schema_names()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_view_names(self, connection, schema=None, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def has_table(self, connection, table_name, schema=None):
        return table_name in connection.connection.get_table_names()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    @reflection.cache
    def get_table_names(self, engine, schema=None, **kw):
        return engine.connect().connection.get_table_names()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_columns(self, engine, table_name, schema=None, **kw):
        cols = engine.connect().connection.get_columns(table_name)

        get_coldef = lambda x, y: {"name": x,
                                   "type": BQDialect.__TYPE_MAPPINGS.get(y, types.Binary()),
                                   "nullable": True,
                                   "default": None}

        return [get_coldef(*col) for col in cols]

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_primary_keys(self, engine, table_name, schema=None, **kw):
        return []

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_foreign_keys(self, engine, table_name, schema=None, **kw):
        return []

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
dialect = BQDialect
