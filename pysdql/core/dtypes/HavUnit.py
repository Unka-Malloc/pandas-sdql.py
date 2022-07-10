class HavUnit:
    def __init__(self, iter_rec, col_name=None):
        self.iter_rec = iter_rec
        self.col_name = col_name

    @property
    def expr(self):
        return f'{self.iter_rec}.{self.col_name}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        if type(item) == str:
            return HavUnit(self.iter_rec, item)

    def __mul__(self, other):
        from pysdql.core.dtypes.HavingExpr import HavExpr
        return HavExpr(self.iter_rec, self, '*', other)
