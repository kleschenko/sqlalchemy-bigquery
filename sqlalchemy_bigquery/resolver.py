import collections


ResolverContext = collections.namedtuple("ResolverContext",
                                         ["cursor", "namespace", "params"])


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class Resolver(object):
    def __call__(self, cursor, namespace, params):
        return self.resolve(ResolverContext(cursor, namespace, params))

    def resolve(self, ctx):
        raise NotImplementedError()


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class CreateTableResolver(Resolver):

    def __init__(self, tablename, colnames, coltypes):
        self.tablename = tablename
        self.colnames = colnames
        self.coltypes = coltypes

    def resolve(self, ctx, **kw):
        raise NotImplementedError()
