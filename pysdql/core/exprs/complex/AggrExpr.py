from pysdql.core.exprs.advanced.AggrOpExprs import (
    AggrOpFilter,
    AggrBinOp,
)

from pysdql.core.enums.EnumUtil import (
    AggrType,
    MathSymbol,
)

from pysdql.core.interfaces.availability.Replaceable import Replaceable

from pysdql.core.killer.SDQLInspector import SDQLInspector

from pysdql.core.prototype.basic.sdql_ir import CompareSymbol

class AggrExpr(Replaceable):
    def __init__(self,
                 aggr_type,
                 aggr_on,
                 aggr_op: dict,
                 aggr_if=None,
                 aggr_else=None,
                 update_sum=False,
                 origin_dict=None,
                 unique_columns=None,
                 is_single_col_op=False,
                 is_multi_col_op=False,
                 is_dict_op=False,
                 is_rec_op=False):
        if origin_dict is None:
            origin_dict = {}
        self.origin_dict = origin_dict
        self.aggr_type = aggr_type
        self.aggr_on = aggr_on
        self.aggr_op = aggr_op

        self.aggr_if = aggr_if
        self.aggr_else = aggr_else

        self.update_sum = update_sum

        self.unique_columns = unique_columns if unique_columns else []

        # ColEl.sum()
        self.is_single_col_op = is_single_col_op
        # ColOpExpr.sum()
        self.is_multi_col_op = is_multi_col_op
        # Groupby Aggregation
        self.is_dict_op = is_dict_op
        # Aggregation
        self.is_rec_op = is_rec_op

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
        return AggrBinOp(self, other, MathSymbol.MUL, self.aggr_on)

    def __rmul__(self, other):
        return AggrBinOp(other, self, MathSymbol.MUL, self.aggr_on)

    def __truediv__(self, other):
        return AggrBinOp(self, other, MathSymbol.DIV, self.aggr_on)

    def __gt__(self, other):
        return AggrOpFilter(self, other, CompareSymbol.GT)

    def __repr__(self):
        return f'{self.aggr_op}'

    def optimize(self):
        return self.aggr_on.optimize()

    def to_sdqlir(self, optimize=True, indent='    ', verbose=True):
        return self.aggr_on.to_sdqlir(optimize=optimize, indent=indent, verbose=verbose)

    def show(self):
        self.aggr_on.show()

    @property
    def descriptor(self) -> str:
        desc = ""

        for k in self.origin_dict.keys():
            if k == 'sum_agg':
                # ColOpExpr AggrExpr
                # first element in the tuple will be sdql_ir object
                desc += SDQLInspector.find_a_descriptor(self.origin_dict[k][0])
            else:
                desc += f'{self.origin_dict[k][0]}_{self.origin_dict[k][1]}'

        return desc
