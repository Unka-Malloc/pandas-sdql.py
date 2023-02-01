from pysdql.core.dtypes.ColExtExpr import ColExtExpr
from pysdql.core.dtypes.sdql_ir import ExtFuncSymbol


class ExtDatetime:
    def __init__(self, col):
        self.col = col

    @property
    def year(self):
        return ColExtExpr(col=self.col, ext_func=ExtFuncSymbol.ExtractYear)


