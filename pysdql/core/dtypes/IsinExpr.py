from pysdql.core.dtypes.ConditionalUnit import CondUnit


class IsinExpr:
    def __init__(self, unit1, unit2):
        self.unit1 = unit1
        self.unit2 = unit2

    @property
    def expr(self):
        return f'{self.unit1} == {self.unit2}'

    def __repr__(self):
        return str(self.cond)

    @property
    def cond(self):
        return CondUnit(self.unit1, '==', self.unit2, inherit_from=self.unit2.relation, isin=True)

    def __and__(self, other):
        return CondUnit(unit1=self.cond,
                        operator='&&',
                        unit2=other).inherit(self.cond).inherit(other)

    def __rand__(self, other):
        return CondUnit(unit1=other,
                        operator='&&',
                        unit2=self.cond).inherit(self.cond).inherit(other)

    def __or__(self, other):
        return CondUnit(unit1=self.cond,
                        operator='||',
                        unit2=other).inherit(self.cond).inherit(other)

    def __ror__(self, other):
        return CondUnit(unit1=other,
                        operator='||',
                        unit2=self.cond).inherit(self.cond).inherit(other)

    def __invert__(self):
        return CondUnit(unit1=self.cond,
                        operator='~',
                        unit2=self.cond).inherit(self.cond)
