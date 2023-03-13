from pysdql.core.dtypes.sdql_ir import IfExpr


class ApplyOpExprUnopt:
    def __init__(self, apply_op, apply_to, apply_cond=None, apply_else=None):
        self.apply_op = apply_op
        self.apply_to = apply_to

        self.apply_cond = apply_cond
        self.apply_else = apply_else

    @property
    def sdql_ir(self):
        if self.apply_cond:
            return IfExpr(self.apply_cond,
                          self.apply_op,
                          self.apply_else)
        else:
            return self.apply_op

    def __repr__(self):
        return str(self.sdql_ir)