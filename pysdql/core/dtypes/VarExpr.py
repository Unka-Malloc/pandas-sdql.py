from pysdql.core.dtypes.OpExpr import OpExpr


class VarExpr:
    def __init__(self, name, data=None, inherit_from=None):
        self.name = name
        self.data = data
        self.inherit_from = inherit_from

    # def rename(self, new_name):
    #     output = VarExpr(self.name, new_name)
    #     self.name = new_name
    #     self.inherit_from.history_name.append(new_name)
    #     self.inherit_from.operations.append(OpExpr('varexpr_rename', output))
    #     return self

    @property
    def expr(self) -> str:
        if self.data:
            return f'let {self.name} = {self.data}'
        return f''

    def __repr__(self):
        return self.expr
