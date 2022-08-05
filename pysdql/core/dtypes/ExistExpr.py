class ExistExpr:
    def __init__(self, main_col, sub_col, conds=None, reverse=False):
        self.main_col = main_col
        self.sub_col = sub_col

        self.main_r = main_col.dataframe
        self.sub_r = sub_col.dataframe

        self.conds = conds

        if not reverse:
            self.flag = 'exists'
        else:
            self.flag = 'not_exists'


