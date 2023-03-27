from pysdql.core.dtypes.FreeStateExpr import FreeStateExpr


class FreeStateVar(FreeStateExpr):
    def __init__(self, var_name, var_value, create_from):
        self.var_name = var_name
        self.var_value = var_value
        self.create_from = create_from

    @property
    def refer(self):
        return self.var_name

    def __repr__(self):
        return str({self.var_name: self.var_value})

    @property
    def op_name_suffix(self):
        return f'_free_state_var'
