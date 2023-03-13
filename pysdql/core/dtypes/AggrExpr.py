from pysdql.core.dtypes.AggrFiltCond import AggrFiltCond
from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import (
    AggrType, MathSymbol, LogicSymbol,
)
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.Utils import input_fmt
from pysdql.core.dtypes.sdql_ir import CompareSymbol


class AggrExpr(FlexIR):
    def __init__(self, aggr_type, aggr_on, aggr_op: dict, aggr_if=None, aggr_else=None, update_sum=False,
                 origin_dict=None, for_calc=False):
        if origin_dict is None:
            origin_dict = {}
        self.origin_dict = origin_dict
        self.aggr_type = aggr_type
        self.aggr_on = aggr_on
        self.aggr_op = aggr_op

        self.aggr_if = aggr_if
        self.aggr_else = aggr_else

        self.update_sum = update_sum

        self.for_calc = for_calc

    @staticmethod
    def rename_aggr_key(aggr_dict, from_name, to_name):
        if len(aggr_dict.keys()) == 1:
            key = list(aggr_dict.keys())[0]
            if key == from_name:
                val = aggr_dict[key]
                aggr_dict = {to_name: val}

        return aggr_dict

    def update_default(self, name):
        self.aggr_op = self.rename_aggr_key(self.aggr_op, 'sum_agg', name)
        self.origin_dict = self.rename_aggr_key(self.origin_dict, 'sum_agg', name)

    def run_in_sdql(self, datasets=None, optimize=True, indent='    '):
        return self.aggr_on.run_in_sdql(
            datasets=datasets,
            optimize=optimize,
            indent=indent,
        )

    @staticmethod
    def ret_for_agg():
        return True

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

    def __rmul__(self, other):
        return CalcExpr(input_fmt(other), input_fmt(self), MathSymbol.MUL, self.aggr_on)

    def __truediv__(self, other):
        return CalcExpr(input_fmt(self), input_fmt(other), MathSymbol.DIV, self.aggr_on)

    def __gt__(self, other):
        return AggrFiltCond(self, other, CompareSymbol.GT)

    def __repr__(self):
        return f'{self.aggr_op}'

    def optimize(self):
        return self.aggr_on.optimize()

    def show(self):
        self.aggr_on.show()
