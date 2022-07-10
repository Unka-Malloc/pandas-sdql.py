class ConstrExpr:
    def __init__(self, iter_expr, any_expr):
        self.iter_expr = iter_expr
        self.any_expr = any_expr

    @property
    def expr(self):
        return f'{self.iter_expr} {self.any_expr}'

    def __repr__(self):
        return self.expr
