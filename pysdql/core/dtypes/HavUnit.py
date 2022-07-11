class HavUnit:
    def __init__(self, iter_expr, col_name=None, groupby_cols=None):
        self.groupby_cols = groupby_cols
        self.iter_expr = iter_expr
        self.col_name = col_name

    def new_expr(self, new_str) -> str:
        return f'{new_str}.{self.col_name}'

    @property
    def expr(self):
        return f'{self.iter_expr.key}.{self.col_name}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        if type(item) == str:
            return HavUnit(self.iter_expr, item, self.groupby_cols)

    def __mul__(self, other):
        from pysdql.core.dtypes.HavingExpr import HavExpr
        return HavExpr(self.iter_expr, self, '*', other, self.groupby_cols)
