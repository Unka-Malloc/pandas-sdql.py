class IterStmt:
    def __init__(self, iter_expr, any_expr):
        self.iter_expr = iter_expr
        self.any_expr = any_expr

    @property
    def expr(self):
        if type(self.iter_expr) == list or type(self.iter_expr) == tuple:
            tmp_str = ''
            for i in self.iter_expr:
                tmp_str += f'{i}'
                tmp_str += ' '
            return f'{tmp_str}{self.any_expr}'
        return f'{self.iter_expr} {self.any_expr}'

    def __repr__(self):
        return self.expr
