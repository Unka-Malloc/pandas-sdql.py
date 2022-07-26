class IterStmt:
    def __init__(self, iter_expr, any_expr, tmp_vars=''):
        self.iter_expr = iter_expr
        self.any_expr = any_expr
        self.tmp_vars = tmp_vars

    @property
    def expr(self):
        if self.tmp_vars:
            if type(self.iter_expr) == list or type(self.iter_expr) == tuple:
                tmp_str = ''
                for i in self.iter_expr:
                    tmp_str += f'{i}'
                    tmp_str += ' '
                return f'{tmp_str}{self.tmp_vars}{self.any_expr}'
            return f'{self.iter_expr} {self.tmp_vars} {self.any_expr}'
        else:
            if type(self.iter_expr) == list or type(self.iter_expr) == tuple:
                tmp_str = ''
                for i in self.iter_expr:
                    tmp_str += f'{i}'
                    tmp_str += ' '
                return f'{tmp_str}{self.any_expr}'
            return f'{self.iter_expr} {self.any_expr}'

    def __repr__(self):
        return self.expr
