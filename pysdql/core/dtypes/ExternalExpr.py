class ExternalExpr:
    def __init__(self, col, ext_func, args=None):
        self.col = col
        self.func = ext_func
        self.args = args

    def new_expr(self, new_str) -> str:
        if self.func == 'Year':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)})'
        if self.func == 'StrStartsWith':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
        if self.func == 'StrEndsWith':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
        if self.func == 'StrContains':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
        if self.func == 'not_StrContains':
            pass
        if self.func == 'SubString':
            return f'ext(`SubString`, {self.col.new_expr(new_str)}, {self.args[0]}, {self.args[1]})'

    @property
    def expr(self):
        if self.func == 'Year':
            return f'ext(`Year`, {self.col})'
        if self.func == 'StrStartsWith':
            return f'ext(`StrStartsWith`, {self.col}, "{self.args}")'
        if self.func == 'StrEndsWith':
            return f'ext(`StrEndsWith`, {self.col}, "{self.args}")'
        if self.func == 'StrContains':
            tmp_str = ''
            for i in range(len(self.args)):
                tmp_str += f'"{self.args[i]}"'
                if i < len(self.args) - 1:
                    tmp_str += ', '
            return f'ext(`StrContainsN`, {self.col}, {tmp_str})'
        if self.func == 'not_StrContains':
            tmp_str = ''
            for i in range(len(self.args)):
                tmp_str += f'"{self.args[i]}"'
                if i < len(self.args) - 1:
                    tmp_str += ', '
            return f'!(ext(`StrContainsN`, {self.col}, {tmp_str}))'
        if self.func == 'SubString':
            return f'ext(`SubString`, {self.col}, {self.args[0]}, {self.args[1]})'

    def __repr__(self):
        return self.expr

    def __invert__(self):
        return f'!({self.expr})'

    def isin(self, vals):
        return self.col.isin(vals, ext=self.expr)
