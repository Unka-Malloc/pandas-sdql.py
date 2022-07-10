class ColExpr:
    def __init__(self, unit1, operator: str, unit2):
        self.unit1 = unit1
        self.op = operator
        self.unit2 = unit2

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
        pass

    @property
    def expr(self):
        return f'({self.unit1} {self.op} {self.unit2})'

    def __repr__(self):
        return self.expr

    def __add__(self, other):
        return ColExpr(unit1=self, operator='+', unit2=other)

    def __radd__(self, other):
        return ColExpr(unit1=other, operator='+', unit2=self)

    def __sub__(self, other):
        return ColExpr(unit1=self, operator='-', unit2=other)

    def __rsub__(self, other):
        return ColExpr(unit1=other, operator='-', unit2=self)

    def __mul__(self, other):
        return ColExpr(unit1=self, operator='*', unit2=other)

    def __rmul__(self, other):
        return ColExpr(unit1=other, operator='*', unit2=self)

    def __div__(self, other):
        return ColExpr(unit1=self, operator='/', unit2=other)

    def __rdiv__(self, other):
        return ColExpr(unit1=other, operator='/', unit2=self)






