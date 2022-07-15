from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VarExpr import VarExpr


class ColExpr:
    def __init__(self, unit1, operator: str, unit2, inherit_from=None):
        self.unit1 = unit1
        self.op = operator
        self.unit2 = unit2
        self.inherit_from = inherit_from

    def new_expr(self, new_str) -> str:
        from pysdql.core.dtypes.ColumnUnit import ColUnit
        if type(self.unit1) == ColExpr or type(self.unit1) == ColUnit:
            u1_str = self.unit1.new_expr(new_str)
        else:
            u1_str = f'{self.unit1}'

        if type(self.unit2) == ColExpr or type(self.unit2) == ColUnit:
            u2_str = self.unit2.new_expr(new_str)
        else:
            u2_str = f'{self.unit2}'

        return f'({u1_str} {self.op} {u2_str})'

    def sum(self):
        tmp_name = f'agg_val'
        if self.inherit_from:
            result = VarExpr(tmp_name, CompoExpr(self.inherit_from.iter_expr, self.inherit_from.iter_expr.val))
            self.inherit_from.history_name.append(tmp_name)
            self.inherit_from.operations.append(OpExpr('colexpr_aggr_sum', result))
        return VarExpr(tmp_name)

    def inherit(self, other):
        if self.inherit_from:
            pass
        else:
            self.inherit_from = other.inherit_from

    @property
    def expr(self):
        return f'({self.unit1} {self.op} {self.unit2})'

    def __repr__(self):
        return self.expr

    def __add__(self, other):
        return ColExpr(unit1=self, operator='+', unit2=other, inherit_from=self.inherit_from)

    def __radd__(self, other):
        return ColExpr(unit1=other, operator='+', unit2=self, inherit_from=self.inherit_from)

    def __sub__(self, other):
        return ColExpr(unit1=self, operator='-', unit2=other, inherit_from=self.inherit_from)

    def __rsub__(self, other):
        return ColExpr(unit1=other, operator='-', unit2=self, inherit_from=self.inherit_from)

    def __mul__(self, other):
        return ColExpr(unit1=self, operator='*', unit2=other, inherit_from=self.inherit_from)

    def __rmul__(self, other):
        return ColExpr(unit1=other, operator='*', unit2=self, inherit_from=self.inherit_from)

    def __div__(self, other):
        return ColExpr(unit1=self, operator='/', unit2=other, inherit_from=self.inherit_from)

    def __rdiv__(self, other):
        return ColExpr(unit1=other, operator='/', unit2=self, inherit_from=self.inherit_from)
