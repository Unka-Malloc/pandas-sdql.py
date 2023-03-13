from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.sdql_ir import CompareSymbol


class AggrFiltCond():
    def __init__(self, aggr_unit1, aggr_unit2, cond_op):
        self.aggr_unit1 = aggr_unit1
        self.aggr_unit2 = aggr_unit2
        self.cond_op = cond_op

    def get_in_pairs(self):
        if isinstance(self.aggr_unit1, CalcExpr):
            return self.cond_op, self.aggr_unit1, self.aggr_unit2
        if isinstance(self.aggr_unit2, CalcExpr):
            tmp_op = self.cond_op
            if tmp_op == CompareSymbol.GT:
                tmp_op = CompareSymbol.LT

            elif tmp_op == CompareSymbol.GTE:
                tmp_op = CompareSymbol.LTE

            elif tmp_op == CompareSymbol.LT:
                tmp_op = CompareSymbol.GT

            elif tmp_op == CompareSymbol.LTE:
                tmp_op = CompareSymbol.GTE

            return tmp_op, self.aggr_unit2, self.aggr_unit1
        else:
            raise IndexError(f'Unable to find pre-calculation value.')

    @property
    def op_name_suffix(self):
        return '_filter'

    def __repr__(self):
        return f'{self.aggr_unit1}\n{self.cond_op}\n{self.aggr_unit2}'
