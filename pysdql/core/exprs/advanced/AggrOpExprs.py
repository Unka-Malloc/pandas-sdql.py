from pysdql.core.enums.EnumUtil import (
    MathSymbol,
    OpRetType,
)

from pysdql.core.interfaces.availability.api import Replaceable

from pysdql.core.exprs.carrier.OpExpr import OpExpr

from pysdql.core.killer.SDQLInspector import SDQLInspector

from pysdql.core.prototype.basic.sdql_ir import (
    RecAccessExpr,

    CompareSymbol,

    DivExpr,
    MulExpr,
)

from pysdql.core.utils.format_utils import input_fmt

"""
AggrUniOp -> AggrExpr -> A.sum()
AggrBinOp -> A.sum() * B.sum()
AggrOpFilter -> DataFrame.filter()
AggrOpRename -> df['B'] = df[df['A'].sum()]
"""

class AggrBinOp(Replaceable):
    def __init__(self, unit1, unit2, op, on, unique_columns=None):
        """
        It should be only generated in AggrExpr.
        :param unit1:
        :param unit2:
        :param op:
        :param on:
        """
        self.unit1 = unit1
        self.unit2 = unit2
        self.op = op
        self.on = on

        self.unique_cols = unique_columns if unique_columns else []

        self.init_rec()

    def init_rec(self):
        if type(self.unit1) == RecAccessExpr:
            self.unit1 = RecAccessExpr(self.on.var_expr, self.unit1.name)
        if type(self.unit2) == RecAccessExpr:
            self.unit2 = RecAccessExpr(self.on.var_expr, self.unit2.name)

        # if isinstance(self.unit2, (AddExpr, MulExpr, SubExpr, DivExpr)):
        #     print(self.unit2)

    def __mul__(self, other):
        return AggrBinOp(self, other, MathSymbol.MUL, self.on)

    def __truediv__(self, other):
        if hasattr(other, 'unique_columns'):
            return AggrBinOp(self, other, MathSymbol.DIV, self.on, unique_columns=other.unique_columns)
        else:
            return AggrBinOp(self, other, MathSymbol.DIV, self.on)

    def replace_aggr(self, target, to_which):
        for k in target.keys():
            if not isinstance(self.unit1, (bool, int, float, str)):
                if SDQLInspector.check_equal_expr(self.unit1.sdql_ir, target[k]):
                    self.unit1 = RecAccessExpr(to_which, k)
            if not isinstance(self.unit2, (bool, int, float, str)):
                if SDQLInspector.check_equal_expr(self.unit2.sdql_ir, target[k]):
                    self.unit2 = RecAccessExpr(to_which, k)

        return self

    @staticmethod
    def unit_fmt(value):
        if isinstance(value, RecAccessExpr):
            return value.name
        elif isinstance(value, (bool, int, float, str)):
            return value
        elif isinstance(value, Replaceable):
            return value.oid
        else:
            return hash(value)

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return hash((
            self.on.name,
            self.op,
            self.unit_fmt(self.unit1),
            self.unit_fmt(self.unit2)
        ))

    @property
    def sdql_ir(self):
        if self.op == MathSymbol.DIV:
            return DivExpr(input_fmt(self.unit1), input_fmt(self.unit2))
        if self.op == MathSymbol.MUL:
            return MulExpr(input_fmt(self.unit1), input_fmt(self.unit2))

    def __repr__(self):
        return f'{self.sdql_ir}'

    def show(self):
        op_expr = OpExpr(op_obj=self,
                         op_on=self.on,
                         op_iter=True,
                         iter_on=self.on,
                         ret_type=OpRetType.FLOAT)

        self.on.push(op_expr)

        return self.on.show()

    def optimize(self):
        op_expr = OpExpr(op_obj=self,
                         op_on=self.on,
                         op_iter=True,
                         iter_on=self.on,
                         ret_type=OpRetType.FLOAT)

        self.on.push(op_expr)

        return self.on.optimize()

    def to_sdqlir(self, optimize=True, indent='    ', verbose=True):
        op_expr = OpExpr(op_obj=self,
                         op_on=self.on,
                         op_iter=True,
                         iter_on=self.on,
                         ret_type=OpRetType.FLOAT)

        self.on.push(op_expr)

        return self.on.to_sdqlir(optimize=optimize, indent=indent, verbose=verbose)

    @property
    def op_name_suffix(self):
        return '_calc'

    def replace(self, rec, inplace=False, mapper=None):
        new_unit1 = self.unit1
        new_unit2 = self.unit2

        if isinstance(self.unit1, Replaceable):
            new_unit1 = self.unit1.replace(rec, inplace, mapper)
        if isinstance(self.unit2, Replaceable):
            new_unit2 = self.unit2.replace(rec, inplace, mapper)

        if self.op == MathSymbol.DIV:
            return DivExpr(input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == MathSymbol.MUL:
            return MulExpr(input_fmt(new_unit1), input_fmt(new_unit2))

    @property
    def descriptor(self):
        return SDQLInspector.find_a_descriptor(self.sdql_ir)

class AggrOpFilter:
    def __init__(self, aggr_unit1, aggr_unit2, cond_op, groupby_cols=None):
        self.groupby_cols = groupby_cols if groupby_cols else []
        self.aggr_unit1 = aggr_unit1
        self.aggr_unit2 = aggr_unit2
        self.cond_op = cond_op

    def get_in_pairs(self):
        if isinstance(self.aggr_unit1, AggrBinOp):
            return self.cond_op, self.aggr_unit1, self.aggr_unit2
        if isinstance(self.aggr_unit2, AggrBinOp):
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

class AggrOpRename:
    def __init__(self, aggr_expr, rename_to, rename_from):
        self.aggr_expr = aggr_expr
        self.rename_to = rename_to
        self.rename_from = rename_from

    @property
    def op_name_suffix(self):
        return '_aggr_rename'
