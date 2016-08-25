from sqlalchemy import sql
from resolver import CreateTableResolver


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQDDLCompiler(sql.compiler.DDLCompiler):

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_create_table(self, create, **kw):
        colnames = [x.name for x in create.element.c]
        coltypes = [x.type for x in create.element.c]
        return CreateTableResolver(create.element.name,
                                   colnames,
                                   coltypes)

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_create_view(self, view, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_drop_view(self, view, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_drop_table(self, drop, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_create_index(self, ind, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_drop_index(self, ind, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_create_constraint(self, ind, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_drop_constraint(self, ind, **kw):
        raise NotImplementedError()

    def get_column_specification(self, column, **kwargs):
        raise NotImplementedError()


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class BQSQLCompiler(sql.compiler.SQLCompiler):

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def get_select_precolumns(self, select, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def limit_clause(self, select, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_column(self, column, add_to_result_map=None, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_match_op_binary(self, binary, operator, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_unary(self, unary, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_function(self, unary, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_label(self, label,
                    add_to_result_map=None,
                    within_label_clause=False,
                    within_columns_clause=False, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_concat_op_binary(self, binary, operator, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_in_op_binary(self, binary, operator, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_is__binary(self, binary, operator, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_isnot_binary(self, binary, operator, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_now_func(self, func, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_count_func(self, func, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_max_func(self, func, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_min_func(self, func, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_null(self, expr, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_true(self, expr, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_false(self, expr, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_clauselist(self, clauselist, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_table(self, table,
                    asfrom=False,
                    iscrud=False,
                    ashint=False,
                    fromhints=None,
                    **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_grouping(self, grouping, asfrom=False, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_alias(self, alias,
                    asfrom=False, ashint=False,
                    iscrud=False,
                    fromhints=None, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_join(self, join, asfrom=False, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_binary(self, binary, override_op=None, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def bindparam_string(self, name, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def order_by_clause(self, select, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_select(self, select,
                     asfrom=False, parens=True,
                     iswrapper=False, fromhints=None,
                     compound_index=0,
                     force_result_map=False,
                     positional_names=None, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_compound_select(self, cs, asfrom=False,
                              parens=True, compound_index=0, **kwargs):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_insert(self, insert_stmt, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_update(self, update_stmt, **kw):
        raise NotImplementedError()

    #  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
    def visit_delete(self, delete_stmt, **kw):
        raise NotImplementedError()
