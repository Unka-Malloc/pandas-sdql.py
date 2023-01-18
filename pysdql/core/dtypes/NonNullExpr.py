from pysdql.core.dtypes.sdql_ir import *


class NonNullExpr:
    def __init__(self, var_dict, var_key):
        self.var_dict = var_dict
        self.var_key = var_key

    @property
    def sdql_ir(self):
        return CompareExpr(CompareSymbol.NE, DicLookupExpr(self.var_dict, self.var_key), ConstantExpr(None))
