

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQString():
    def __init__(self,
                length=None,
                collation=None,
                convert_unicode=False,
                unicode_error=None,
                _warn_on_bytestring=False):

        prnt = super(BQString, self)
        return prnt.__init__(length=length,
                             collation=collation,
                             convert_unicode=convert_unicode,
                             unicode_error=unicode_error,
                             _warn_on_bytestring=_warn_on_bytestring)

    def get_col_spec(self):
        return "STRING"

    def literal_processor(self, dialect):
        def process(value):
            value = value.replace("'", "\\'")
            return "'{0}'".format(value)
        return process


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQInteger():
    def get_col_spec(self):
        return "INTEGER"


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQFloat():
    def get_col_spec(self):
        return "FLOAT"


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQBytes():
    def get_col_spec(self):
        return "BYTES"


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQBoolean():
    def get_col_spec(self):
        return "BOOLEAN"


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQTimestamp():
    def get_col_spec(self):
        return "TIMESTAMP"
