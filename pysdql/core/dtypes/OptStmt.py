from pysdql.core.dtypes.sdql_ir import (
    LetExpr,
    VarExpr,
    SumBuilder,
)


class OptStmt:
    def __init__(self, opt_name, opt_sum):
        self.opt_name = opt_name
        self.opt_sum = opt_sum

    @property
    def sdql_ir(self):
        return
