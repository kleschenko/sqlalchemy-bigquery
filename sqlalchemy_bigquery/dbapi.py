'''
Created on 16 Aug 2016

'''
import exceptions
import json
import compat
from datetime import datetime
from collections import deque
from datapath.api.bquery import (get_auth, submit_query,
                                 next_pg_query_result,
                                 list_tables,
                                 get_columns,
                                 build_query_job_def,
                                 list_datasets)
apilevel = "1.0"
threadsafety = 2
paramstyle = 'named'

QUERY_TIMEOUT_SEC = 3600
QUERY_NUM_RETRIES = 5

TYPE_MAPPINGS = {'TIMESTAMP': lambda x: datetime.utcfromtimestamp(float(x)) if x is not None else None,
                 'STRING': lambda x: str(x) if x is not None else None,
                 'FLOAT': lambda(x): float(x) if x is not None else None,
                 'INTEGER': lambda x: int(x) if x is not None else None}


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def map_values(bigquery_rows,
               mapping_functions=None):
    #
    # First iterate over each row; each item is tagged with 'f'
    #
    perRowIterator = (x['f'] for x in bigquery_rows)
    #
    # Now apply the mapping function based on the data type provided by BigQuery
    #
    if mapping_functions:
        #
        # We *do* have a mapping - apply it as we go
        #
        mapped_values = [[f(y['v']) for f, y in zip(mapping_functions, x)] for x in perRowIterator]
    else:
        #
        # We don't have a mapping; just present what we got
        #
        mapped_values = [[y['v'] for y in x] for x in perRowIterator]
    #
    # Return output
    #
    return mapped_values


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def connect(project=None,
            dataset=None,
            jsonAuthFile=None):

    if any([x is None for x in [project, dataset, jsonAuthFile]]):
        raise ProgrammingError('Error: Inputs to connect must be non-null.')

    return Connection(project, dataset, jsonAuthFile)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Connection(object):

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def __init__(self, project, dataset, jsonAuthFile,
                 query_timeout=QUERY_TIMEOUT_SEC,
                 query_retries=QUERY_NUM_RETRIES):
        #
        # Set initial state
        #
        self.project = project
        self.dataset = dataset
        self.query_timeout = query_timeout
        self.query_retries = query_retries
        self._namespace = '{0}:{1}'.format(project, dataset)
        self.auth = get_auth(jsonAuthFile)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def close(self):
        #
        # Nothing to do here, as requests are authenticated individually
        # This is provided just for homogeneity
        #
        pass

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def commit(self):
        #
        # Nothing to do here, as requests are autocommitted
        # This is provided just for homogeneity
        #
        pass

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def rollback(self):
        #
        # Nothing to do here, as requests are autocommitted
        # This is provided just for homogeneity
        #
        raise NotSupportedError('Google BigQuery does not support rollbacks.')

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def cursor(self):
        #
        # Instantiate and return directly
        #
        return Cursor(self)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_table_names(self):
        return list_tables(self.auth, self.project, self.dataset)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_columns(self, table_name):
        return get_columns(self.auth, self.project, self.dataset, table_name)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_schema_names(self):
        return list_datasets(self.auth, self.project)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Cursor(object):

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def __init__(self, connection):
        self.connection = connection
        self.clear_state()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def clear_state(self):
        self.result = None
        self.read_rows = deque()
        self.description = []
        self.rowcount = 0
        self.job_id = None
        self.pages_processed = set()
        self.lastrowid = 0
        self.mapping_functions = None

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def execute(self,
                stmt,
                params=None,
                legacy_sql=False):

        self.clear_state()

        if isinstance(stmt, compat.basestring):

            try:
                job_definition = build_query_job_def(self.connection.project,
                                                     stmt,
                                                     batchMode=False,
                                                     useLegacySQL=legacy_sql,
                                                     createDisposition='CREATE_IF_NEEDED',
                                                     writeDisposition='WRITE_APPEND',
                                                     destinationTable=None,
                                                     allowLargeResults=False,
                                                     returnFullSet=True,
                                                     debug=False)
                #
                # We interpret stmt as an SQL query and run it
                #
                (query_successful,
                 job_id,
                 query_result) = submit_query(self.connection.project,
                                              {'projectId': self.connection.project,
                                               'body': job_definition},
                                              self.connection.auth,
                                              nRetries=self.connection.query_retries,
                                              timeout=self.connection.query_timeout)
            except Exception as e:
                print(str(e))
                raise

            if (query_successful and
               'rows' in query_result and
               'schema' in query_result and
               'fields' in query_result['schema']):

                self.result = {x: query_result[x] for x in query_result if x != 'rows'}
                self.description = [(k['name'], k['type'], None, None, None, None, None)
                                    for k in query_result['schema']['fields']]
                self.mapping_functions = [TYPE_MAPPINGS[y] for y in (x[1] for x in self.description)]
                self.read_rows = deque(map_values(query_result['rows'],
                                                  mapping_functions=self.mapping_functions))
                self.rowcount = int(query_result['totalRows'])
                self.job_id = job_id
                self.pages_processed = set()
                self.lastrowid += len(self.read_rows)

            else:
                raise DatabaseError('Query Error:{0}'.format(json.dumps(query_result,
                                                                        indent=4)))
        else:
            #
            # For now we assume that these are non-sql interactions
            #
            error_mess = 'Error: unsupported request {0} of type {1}.'
            raise NotSupportedError(error_mess.format(stmt, type(stmt)))

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def executemany(self, stmt, multiparams):
        for param in multiparams:
            self.execute(stmt, param)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def fetchone(self):
        #
        # If we have too few elements, we need a new results page
        #
        if len(self.read_rows) == 0:
            #
            # Verify if we have finished paging. If so, terminate iteration
            #
            if self.rowcount == self.lastrowid:
                #
                # Terminate
                #
                return None
            #
            # We have not finished yet. Then, fetch next page
            #
            (query_result,
             pages_processed) = next_pg_query_result(self.connection.auth,
                                                     self.result,
                                                     self.connection.project,
                                                     self.job_id,
                                                     self.connection.query_retries,
                                                     pagesProcessed=self.pages_processed)
            #
            # Update state based on new page
            #
            self.pages_processed = self.pages_processed.union(pages_processed)

            self.read_rows = deque(map_values(query_result['rows'],
                                              mapping_functions=self.mapping_functions))
            self.lastrowid += len(self.read_rows)
            self.result = {x: query_result[x] for x in query_result if x != 'rows'}

        return self.read_rows.popleft()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def fetchmany(self, size=None):
        size = size or 1
        #
        # Fetchone should be tolerable, so we simply use it
        #
        results = []
        for _ in range(size):
            results.append(self.fetchone())
        #
        # Return outcome
        #
        return results

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def fetchall(self):
        return self.fetchmany(size=self.rowcount)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def close(self):
        pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Error(exceptions.StandardError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Warning(exceptions.StandardError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class InterfaceError(Error):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class DatabaseError(Error):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class InternalError(DatabaseError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class OperationalError(DatabaseError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class ProgrammingError(DatabaseError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class IntegrityError(DatabaseError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class DataError(DatabaseError):
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class NotSupportedError(DatabaseError):
    pass
