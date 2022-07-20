class ExtExpr:
    def __init__(self, col, ext_func, *args):
        self.col = col
        self.func = ext_func
        self.args = args

        print(self.args)

    def new_expr(self, new_str) -> str:
        if self.func == 'Year':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)})'
        if self.func == 'StrStartsWith':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args[0]}")'
        if self.func == 'StrEndsWith':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args[0]}")'
        if self.func == 'StrContains':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args[0]}")'

    @property
    def expr(self):
        if self.func == 'Year':
            return f'ext(`{self.func}`, {self.col})'
        if self.func == 'StrStartsWith':
            return f'ext(`{self.func}`, {self.col}, "{self.args[0]}")'
        if self.func == 'StrEndsWith':
            return f'ext(`{self.func}`, {self.col}, "{self.args[0]}")'
        if self.func == 'StrContains':
            return f'ext(`{self.func}`, {self.col}, "{self.args[0]}")'

    def __repr__(self):
        return self.expr
