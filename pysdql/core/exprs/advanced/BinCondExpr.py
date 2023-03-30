from pysdql.core.enums.EnumUtil import (
    LogicSymbol,
)

from pysdql.core.interfaces.availability.api import (
    Flexible,
    Retrivable,
    Replaceable,
    Transformable,
)

from pysdql.core.interfaces.identifier.api import (
    IgnoreThisFlag,
)

from pysdql.core.utils.format_utils import (
    input_fmt
)

from pysdql.core.prototype.basic.sdql_ir import (
    CompareSymbol,
    CompareExpr,
    MulExpr,
    AddExpr,
    ConstantExpr,
)

from pysdql.core.exprs.advanced.AggrOpExprs import AggrBinOp

from pysdql.core.killer.SDQLInspector import SDQLInspector


class BinCondExpr(Replaceable):
    def __init__(self, unit1, operator, unit2, is_apply_cond=False):
        self.unit1 = unit1
        self.op = operator
        self.unit2 = unit2
        self.is_apply_cond = is_apply_cond

    def __eq__(self, other):
        return BinCondExpr(unit1=self,
                           operator=CompareSymbol.EQ,
                           unit2=other)

    '''
    AND, OR, NOT
    '''

    def __and__(self, other):
        if isinstance(other, IgnoreThisFlag):
            return self
        return BinCondExpr(unit1=self,
                           operator=LogicSymbol.AND,
                           unit2=other)

    def __or__(self, other):
        return BinCondExpr(unit1=self,
                           operator=LogicSymbol.OR,
                           unit2=other)

    def __invert__(self):
        return BinCondExpr(unit1=self,
                           operator=LogicSymbol.NOT,
                           unit2=self)

    '''
    Reverse AND, OR
    '''

    def __rand__(self, other):
        return BinCondExpr(unit1=other,
                           operator=LogicSymbol.AND,
                           unit2=self)
        # return MulExpr(input_fmt(other), self.sdql_ir)

    def __ror__(self, other):
        return BinCondExpr(unit1=other,
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

        if isinstance(self.unit1, Replaceable):
            if isinstance(self.unit1, AggrBinOp):
                new_unit1 = SDQLInspector.replace_access(self.unit1.sdql_ir, rec, inplace)
            else:
                new_unit1 = self.unit1.replace(rec, inplace, mapper)
        if isinstance(self.unit2, Replaceable):
            if isinstance(self.unit2, AggrBinOp):
                new_unit2 = SDQLInspector.replace_access(self.unit2.sdql_ir, rec, inplace)
            else:
                new_unit2 = self.unit2.replace(rec, inplace, mapper)

        if isinstance(self.op, CompareSymbol):
            return CompareExpr(self.op, input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == LogicSymbol.AND:
            return MulExpr(input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == LogicSymbol.OR:
            return AddExpr(input_fmt(new_unit1), input_fmt(new_unit2))
        if self.op == LogicSymbol.NOT:
            return CompareExpr(CompareSymbol.EQ, input_fmt(new_unit1), ConstantExpr(False))

    def findall_tmp_vars(self):
        if isinstance(self.unit1, AggrBinOp):
            pass

        if isinstance(self.unit2, AggrBinOp):
            pass

    @staticmethod
    def unit_fmt(value):
        if isinstance(value, (bool, int, float, str)):
            return value
        elif isinstance(value, (Replaceable)):
            return value.oid
        else:
            return hash(value)

    '''
    Replaceable
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

    
