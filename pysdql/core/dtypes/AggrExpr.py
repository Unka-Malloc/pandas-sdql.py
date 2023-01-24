from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import (
    AggrType, MathSymbol,
)
from pysdql.core.dtypes.SDQLIR import SDQLIR
from pysdql.core.dtypes.Utils import input_fmt


class AggrExpr(SDQLIR):
    def __init__(self, aggr_type, aggr_on, aggr_op: dict, aggr_if=None, aggr_else=None, update_sum=False):
        self.aggr_type = aggr_type
        self.aggr_on = aggr_on
        self.aggr_op = aggr_op

        self.aggr_if = aggr_if
        self.aggr_else = aggr_else

        self.update_sum = update_sum

    @property
    def operations(self):
        return self.aggr_on.operations

    @property
    def op_name_suffix(self):
        return f'_aggr'

    @property
    def sdql_ir(self):
        return self.aggr_op

    def __mul__(self, other):
        return CalcExpr(input_fmt(self), input_fmt(other), MathSymbol.MUL, self.aggr_on)

    def __truediv__(self, other):
        return CalcExpr(input_fmt(self), input_fmt(other), MathSymbol.DIV, self.aggr_on)

    def __repr__(self):
        return f'{self.aggr_op}'

    def optimize(self):
        return self.aggr_on.optimize()

    def show(self):
        self.aggr_on.show()

