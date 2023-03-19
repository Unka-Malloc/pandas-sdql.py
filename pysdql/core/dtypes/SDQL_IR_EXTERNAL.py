from pysdql.core.dtypes.sdql_ir import (
    ConstantExpr,
    VarExpr,
    SumExpr,
    RecAccessExpr,
    PairAccessExpr,
)

from pysdql.extlib.sdqlpy.sdql_lib import sr_dict

class UniqueExpr:
    def __init__(self, col_expr):
        self.col_expr = col_expr

class UniqueBuild:
    def __init__(self, col_op):
        self.col_op = col_op

    @property
    def sdql_ir(self):
        return sr_dict({self.col_op: ConstantExpr(True)})
        # return DicConsExpr([(self.col_op, ConstantExpr(True))])

    def __repr__(self):
        return str(self.sdql_ir)

class UniqueProbe:
    def __init__(self, col_op):
        self.col_op = col_op

    @property
    def sdql_ir(self):
        tmp_var = VarExpr(f'y')
        return SumExpr(varExpr=tmp_var,
                       dictExpr=self.col_op,
                       bodyExpr=PairAccessExpr(tmp_var, 0),
                       isAssignmentSum=True)

    def __repr__(self):
        return str(self.sdql_ir)