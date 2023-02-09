from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.VarExpr import VarExpr

from pysdql.core.dtypes.sdql_ir import (
    AddExpr,
    MulExpr,
    SubExpr,
    DivExpr, ConstantExpr,
)

from pysdql.core.dtypes.EnumUtil import (
    MathSymbol, AggrType, OpRetType,
)

from pysdql.core.dtypes.Utils import (
    input_fmt,
)


class ColOpExpr(FlexIR):
    def __init__(self, unit1, operator, unit2):
        self.unit1 = unit1
        self.operator = operator
        self.unit2 = unit2

    def sum(self):
        aggr_expr = AggrExpr(aggr_type=AggrType.Scalar,
                             aggr_on=None,
                             aggr_op={f'sum_{self.oid}': self.sdql_ir},
                             aggr_else=ConstantExpr(0.0),
                             origin_dict={f'sum_{self.oid}': (self.sdql_ir, 'sum')})

        return aggr_expr

    '''
    Arithmetic Operations
    '''

    def __add__(self, other):
        return ColOpExpr(unit1=self,
                         operator=MathSymbol.ADD,
                         unit2=other)
        # return ColExpr(value=AddExpr(self.col, input_fmt(other)), relation=self.relation)

    def __mul__(self, other):
        return ColOpExpr(unit1=self,
                         operator=MathSymbol.MUL,
                         unit2=other)
        # return ColExpr(value=MulExpr(self.col, input_fmt(other)), relation=self.relation)

    def __sub__(self, other):
        return ColOpExpr(unit1=self,
                         operator=MathSymbol.SUB,
                         unit2=other)
        # return ColExpr(value=SubExpr(self.col, input_fmt(other)), relation=self.relation)

    def __truediv__(self, other):
        return ColOpExpr(unit1=self,
                         operator=MathSymbol.DIV,
                         unit2=other)
        # return ColExpr(value=DivExpr(self.col, input_fmt(other)), relation=self.relation)

    '''
    Reverse Arithmetic Operations
    '''

    def __radd__(self, other):
        return ColOpExpr(unit1=other,
                         operator=MathSymbol.ADD,
                         unit2=self)
        # return ColExpr(value=AddExpr(input_fmt(other), self.col), relation=self.relation)

    def __rmul__(self, other):
        return ColOpExpr(unit1=other,
                         operator=MathSymbol.MUL,
                         unit2=self)
        # return ColExpr(value=MulExpr(input_fmt(other), self.col), relation=self.relation)

    def __rsub__(self, other):
        return ColOpExpr(unit1=other,
                         operator=MathSymbol.SUB,
                         unit2=self)
        # return ColExpr(value=SubExpr(input_fmt(other), self.col), relation=self.relation)

    def __rtruediv__(self, other):
        return ColOpExpr(unit1=other,
                         operator=MathSymbol.DIV,
                         unit2=self)
        # return ColExpr(value=DivExpr(input_fmt(other), self.col), relation=self.relation)

    def replace(self, rec, inplace=False, mapper=None):
        new_unit1 = self.unit1
        new_unit2 = self.unit2

        if isinstance(self.unit1, FlexIR):
            new_unit1 = self.unit1.replace(rec, inplace, mapper)
        if isinstance(self.unit2, FlexIR):
            new_unit2 = self.unit2.replace(rec, inplace, mapper)

        if self.operator == MathSymbol.ADD:
            return AddExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.MUL:
            return MulExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.SUB:
            return SubExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.DIV:
            return DivExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        raise NotImplemented

    @staticmethod
    def unit_fmt(value):
        if isinstance(value, (bool, int, float, str)):
            return value
        elif isinstance(value, (FlexIR)):
            return value.oid
        else:
            return hash(value)

    '''
    FlexIR
    '''
    @property
    def replaceable(self):
        return True

    @property
    def oid(self):
        return hash((
            self.operator,
            self.unit_fmt(self.unit1),
            self.unit_fmt(self.unit2),
        ))

    @property
    def sdql_ir(self):
        if self.operator == MathSymbol.ADD:
            return AddExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.MUL:
            return MulExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.SUB:
            return SubExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.DIV:
            return DivExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        raise NotImplemented

    def __repr__(self):
        return repr(self.sdql_ir)
