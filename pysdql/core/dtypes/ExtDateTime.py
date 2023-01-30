from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.sdql_ir import ExtFuncSymbol


class ExtDatetime:
    def __init__(self, col):
        self.col = col

    @property
    def year(self):
        return ExternalExpr(col=self.col, ext_func=ExtFuncSymbol.ExtractYear, )
