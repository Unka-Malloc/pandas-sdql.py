from pysdql.core.dtypes.EnumUtil import (
    AggrType,
)
from pysdql.core.dtypes.SDQLIR import SDQLIR


class AggrExpr(SDQLIR):
    def __init__(self, aggr_type, aggr_on, aggr_op, aggr_if=None, aggr_then=None, aggr_else=None, update_sum=False):
        self.aggr_type = aggr_type
        self.aggr_on = aggr_on
        self.aggr_expr = aggr_op

        self.aggr_if = aggr_if
        self.aggr_then = aggr_then
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
        return

    def __repr__(self):
        return f'{self.aggr_expr}'

    def optimize(self):
        return self.aggr_on.optimize()

