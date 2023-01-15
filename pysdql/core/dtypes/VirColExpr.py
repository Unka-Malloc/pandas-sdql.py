class VirColExpr:
    def __init__(self, col_var, col_expr):
        self.col_var = col_var
        self.col_expr = col_expr

    def replace(self, rec, on=None):
        return self.col_expr.replace(rec, on)

    @property
    def expr(self):
        return str({self.col_var: self.col_expr})

    def __str__(self):
        return self.expr

    def __repr__(self):
        return self.expr

    @property
    def op_name_suffix(self):
        return f'_insert'