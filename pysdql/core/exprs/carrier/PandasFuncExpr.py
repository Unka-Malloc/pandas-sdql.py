from pysdql.core.exprs.advanced.ColOpExprs import ColOpExternal

from pysdql.core.prototype.basic.sdql_ir import ExtFuncSymbol

class DropDuplFunc:
    def __init__(self, unique_cols=None):
        self.unique_cols = unique_cols if unique_cols else []

    @property
    def op_name_suffix(self):
        return f'_drop_dup'


class DateTimeProperty:
    def __init__(self, col):
        self.col = col

    @property
    def year(self):
        return ColOpExternal(col=self.col, ext_func=ExtFuncSymbol.ExtractYear)