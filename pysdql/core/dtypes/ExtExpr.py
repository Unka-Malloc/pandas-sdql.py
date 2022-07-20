class ExtExpr:
    def __init__(self, col, ext_func, args):
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

    @property
    def expr(self):
        if self.func == 'Year':
            return f'ext(`{self.func}`, {self.col})'
        if self.func == 'StrStartsWith':
            return f'ext(`{self.func}`, {self.col}, "{self.args}")'
        if self.func == 'StrEndsWith':
            return f'ext(`{self.func}`, {self.col}, "{self.args}")'
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

    def __repr__(self):
        return self.expr
