class DropDupOpExpr:
    def __init__(self, unique_cols=None):
        self.unique_cols = unique_cols if unique_cols else []

    @property
    def op_name_suffix(self):
        return f'_drop_dup'