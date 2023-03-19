class NewColListExpr:
    def __init__(self, col_var, col_list):
        self.col_var = col_var
        self.col_list = col_list

    @property
    def op_name_suffix(self):
        return f'_col_as_list'

    def __getitem__(self, item):
        return self.col_list[item]

    def __repr__(self):
        return str({self.col_var: self.col_list})