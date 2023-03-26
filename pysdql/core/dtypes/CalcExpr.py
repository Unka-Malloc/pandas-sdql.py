from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.EnumUtil import (
    MathSymbol,
    OpRetType
)
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.dtypes.Utils import input_fmt
from pysdql.core.dtypes.sdql_ir import ConstantExpr, DivExpr, MulExpr, RecAccessExpr, SubExpr, AddExpr


class CalcExpr(FlexIR):
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
        return CalcExpr(self, other, MathSymbol.MUL, self.on)

    def __truediv__(self, other):
        if hasattr(other, 'unique_columns'):
            return CalcExpr(self, other, MathSymbol.DIV, self.on, unique_columns=other.unique_columns)
        else:
            return CalcExpr(self, other, MathSymbol.DIV, self.on)

    def replace_aggr(self, target, to_which):

        for k in target.keys():
            if SDQLInspector.check_equal_expr(self.unit1.sdql_ir, target[k]):
                self.unit1 = RecAccessExpr(to_which.var_expr, k)
            if SDQLInspector.check_equal_expr(self.unit2.sdql_ir, target[k]):
                self.unit2 = RecAccessExpr(to_which.var_expr, k)

        return self

    @staticmethod
    def unit_fmt(value):
        if isinstance(value, RecAccessExpr):
            return value.name
        elif isinstance(value, (bool, int, float, str)):
            return value
        elif isinstance(value, FlexIR):
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

    def to_sdqlir(self, optimize=True, indent='    '):
        op_expr = OpExpr(op_obj=self,
                         op_on=self.on,
                         op_iter=True,
                         iter_on=self.on,
                         ret_type=OpRetType.FLOAT)

        self.on.push(op_expr)

        return self.on.to_sdqlir(optimize=optimize, indent=indent)

    @property
    def op_name_suffix(self):
        return '_calc'

    def replace(self, rec, inplace=False, mapper=None):
        new_unit1 = self.unit1
        new_unit2 = self.unit2

        if isinstance(self.unit1, FlexIR):
            new_unit1 = self.unit1.replace(rec, inplace, mapper)
        if isinstance(self.unit2, FlexIR):
            new_unit2 = self.unit2.replace(rec, inplace, mapper)

        if self.op == MathSymbol.DIV:
            return DivExpr(input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == MathSymbol.MUL:
            return MulExpr(input_fmt(new_unit1), input_fmt(new_unit2))
