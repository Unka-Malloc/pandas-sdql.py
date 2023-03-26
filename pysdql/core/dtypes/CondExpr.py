from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr
from pysdql.core.dtypes.Utils import (
    input_fmt
)
from pysdql.core.dtypes.sdql_ir import (
    CompareSymbol,
    CompareExpr,
    MulExpr,
    AddExpr,
    ConstantExpr,
)

from pysdql.core.dtypes.EnumUtil import (
    LogicSymbol,
)


class CondExpr(FlexIR):
    def __init__(self, unit1, operator, unit2, is_apply_cond=False):
        self.unit1 = unit1
        self.op = operator
        self.unit2 = unit2
        self.is_apply_cond = is_apply_cond

    def __eq__(self, other):
        return CondExpr(unit1=self,
                        operator=CompareSymbol.EQ,
                        unit2=other)

    '''
    AND, OR, NOT
    '''

    def __and__(self, other):
        if isinstance(other, IgnoreExpr):
            return self
        return CondExpr(unit1=self,
                        operator=LogicSymbol.AND,
                        unit2=other)

    def __or__(self, other):
        return CondExpr(unit1=self,
                        operator=LogicSymbol.OR,
                        unit2=other)

    def __invert__(self):
        return CondExpr(unit1=self,
                        operator=LogicSymbol.NOT,
                        unit2=self)

    '''
    Reverse AND, OR
    '''

    def __rand__(self, other):
        return CondExpr(unit1=other,
                        operator=LogicSymbol.AND,
                        unit2=self)
        # return MulExpr(input_fmt(other), self.sdql_ir)

    def __ror__(self, other):
        return CondExpr(unit1=other,
                        operator=LogicSymbol.OR,
                        unit2=self)
        # return AddExpr(input_fmt(other), self.sdql_ir)

    '''
    Recursive AND, OR
    '''

    def __iand__(self, other):
        return self & other

    def __ior__(self, other):
        return self | other

    def replace(self, rec, inplace=False, mapper=None):
        new_unit1 = self.unit1
        new_unit2 = self.unit2

        if isinstance(self.unit1, FlexIR):
            new_unit1 = self.unit1.replace(rec, inplace, mapper)
        if isinstance(self.unit2, FlexIR):
            new_unit2 = self.unit2.replace(rec, inplace, mapper)

        if isinstance(self.op, CompareSymbol):
            return CompareExpr(self.op, input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == LogicSymbol.AND:
            return MulExpr(input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == LogicSymbol.OR:
            return AddExpr(input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == LogicSymbol.NOT:
            return CompareExpr(CompareSymbol.EQ, input_fmt(new_unit1), ConstantExpr(False))

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
            self.op,
            self.unit_fmt(self.unit1),
            self.unit_fmt(self.unit2)
        ))

    @property
    def sdql_ir(self):
        if isinstance(self.op, CompareSymbol):
            return CompareExpr(self.op, input_fmt(self.unit1), input_fmt(self.unit2))

        if self.op == LogicSymbol.AND:
            return MulExpr(input_fmt(self.unit1), input_fmt(self.unit2))
        if self.op == LogicSymbol.OR:
            return AddExpr(input_fmt(self.unit1), input_fmt(self.unit2))
        if self.op == LogicSymbol.NOT:
            return CompareExpr(CompareSymbol.EQ, input_fmt(self.unit1), input_fmt(False))

    @property
    def op_name_suffix(self):
        return f'_filter'

    def __repr__(self):
        return repr(self.sdql_ir)
