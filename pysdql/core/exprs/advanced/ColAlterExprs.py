class OldColRename:
    def __init__(self, col_var, col_expr):
        self.col_var = col_var
        self.col_expr = col_expr

    def replace(self, rec, inplace=False, mapper=None):
        return self.col_expr.replace(rec, inplace, mapper)

    @property
    def expr(self):
        return str({self.col_var: self.col_expr})

    def __str__(self):
        return self.expr

    def __repr__(self):
        return self.expr

    @property
    def op_name_suffix(self):
        return f'_old_col_rename'


class NewColInsert:
    def __init__(self, col_var: str, col_expr):
        self.col_var = col_var
        self.col_expr = col_expr

    def replace(self, rec, inplace=False, mapper=None):
        return self.col_expr.replace(rec, inplace, mapper)

    @property
    def expr(self):
        return str({self.col_var: self.col_expr})

    def __str__(self):
        return self.expr

    def __repr__(self):
        return self.expr

    @property
    def op_name_suffix(self):
        return f'_new_col_insert'


class NewColListInsert:
    def __init__(self, col_var, col_list):
        self.col_var = col_var
        self.col_list = col_list

    def __getitem__(self, item):
        return self.col_list[item]

    def __repr__(self):
        return str({self.col_var: self.col_list})

    @property
    def op_name_suffix(self):
        return f'_new_col_insert_as_list'