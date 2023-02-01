from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import (
    AggrType, MathSymbol,
)
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.Utils import input_fmt


class AggrExpr(FlexIR):
    def __init__(self, aggr_type, aggr_on, aggr_op: dict, aggr_if=None, aggr_else=None, update_sum=False,
                 origin_dict=None):
        if origin_dict is None:
            origin_dict = {}
        self.origin_dict = origin_dict
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

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return hash((
            self.aggr_on.name,
            tuple(self.origin_dict.items()),
            self.aggr_type,
        ))

    @property
    def sdql_ir(self):
        if self.aggr_type == AggrType.Scalar:
            return list(self.aggr_op.values())[0]
        else:
            raise ValueError()

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

