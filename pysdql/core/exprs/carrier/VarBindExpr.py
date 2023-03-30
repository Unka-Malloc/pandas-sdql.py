from pysdql.core.interfaces.availability.Replaceable import Replaceable
from pysdql.core.prototype.basic.sdql_ir import (
    LetExpr,
    ConstantExpr
)


class VarBindExpr(Replaceable):
    def __init__(self, var_expr, var_value, next_expr=None):
        self.var_expr = var_expr
        self.var_value = var_value
        self.next_expr = next_expr

    @property
    def iscompleted(self):
        if self.next_expr:
            return True
        else:
            return False

    @property
    def binding(self):
        return LetExpr(varExpr=self.var_expr,
                       valExpr=self.var_value,
                       bodyExpr=self.next_expr)

    def concat(self, other):
        if type(other) == VarBindExpr:
            return VarBindExpr(self.var_expr, self.var_value, other.sdql_ir)
        if type(other) == LetExpr:
            return VarBindExpr(self.var_expr, self.var_value, other)
        if type(other) == ConstantExpr:
            return VarBindExpr(self.var_expr, self.var_value, other)
        raise ValueError()

    def fillin(self, other):
        if type(other) == VarBindExpr:
            return other.concat(self)
        raise ValueError()

    def __add__(self, other):
        return self.concat(other)

    def __iadd__(self, other):
        return self.concat(other)

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return True

    @property
    def sdql_ir(self):
        return self.binding

    def __repr__(self):
        return f'{self.var_expr} = {self.var_value}'
