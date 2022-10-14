class ExternalExpr:
    def __init__(self, col, ext_func, args=None, isinvert=False):
        self.col = col
        self.func = ext_func
        self.args = args

        self.with_cond = None

        self.vars = {}

        self.isinvert = isinvert

    @property
    def vars_str(self):
        tmp_list = []
        if self.vars:
            tmp_dict = self.vars
            for k in tmp_dict.keys():
                tmp_list.append(f'let {k} = {tmp_dict[k]}')
        return ' '.join(tmp_list)

    def new_expr(self, new_str) -> str:
        if self.func == 'Year':
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)})'
        if self.func == 'StrStartsWith':
            if self.isinvert:
                return f'!(ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}"))'
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
        if self.func == 'StrEndsWith':
            if self.isinvert:
                return f'!(ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}"))'
            return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
        if self.func == 'StrContains':
            tmp_str = ''
            for i in range(len(self.args)):
                tmp_str += f'"{self.args[i]}"'
                if i < len(self.args) - 1:
                    tmp_str += ', '
            if self.isinvert:
                return f'!(ext(`StrContainsN`, {self.col.new_expr(new_str)}, {tmp_str}))'
            return f'ext(`StrContainsN`, {self.col.new_expr(new_str)}, {tmp_str})'
        if self.func == 'not_StrContains':
            pass
        if self.func == 'SubString':
            return f'ext(`SubString`, {self.col.new_expr(new_str)}, {self.args[0]}, {self.args[1]})'
        if self.func == 'StrContains_in_order':
            for i in range(len(self.args)):
                if i == 0:
                    self.vars[f'idx{i}'] = f'ext(`StrIndexOf`, {self.col.new_expr(new_str)}, "{self.args[i]}", {i})'
                else:
                    self.vars[f'idx{i}'] = f'ext(`StrIndexOf`, {self.col.new_expr(new_str)}, "{self.args[i]}", idx{i - 1})'
            tmp_str = ''
            for k in range(len(self.vars.keys())):
                tmp_str += f'({list(self.vars.keys())[k]} != -1)'
                if k != len(self.vars.keys()) - 1:
                    tmp_str += f' && '
            if self.isinvert:
                return f'!({tmp_str})'
            else:
                return tmp_str
        if self.func == 'StrIndexOf':
            return f'ext(`StrIndexOf`, {self.col.new_expr(new_str)}, "{self.args[0]}", {self.args[1]})'

    @property
    def expr(self):
        if self.func == 'Year':
            return f'ext(`Year`, {self.col})'
        if self.func == 'StrStartsWith':
            return f'ext(`StrStartsWith`, {self.col.relation.iter_expr.key}.{self.col.field}, "{self.args}")'
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
        if self.func in ('StrStartsWith', 'StrEndsWith', 'StrContains'):
            return f'!({self.expr})'
        else:
            self.isinvert = True
            return self

    def isin(self, vals):
        return self.col.isin(vals, ext=self.expr)
