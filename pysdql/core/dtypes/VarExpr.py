class VarExpr:
    def __init__(self, name, data=None, inherit_from=None):
        self.name = name
        self.data = data
        self.inherit_from = inherit_from

    def rename(self, new_name):
        self.name = new_name

    @property
    def expr(self) -> str:
        if self.data:
            return f'let {self.name} = {self.data}'
        return self.name

    def __repr__(self):
        return self.expr
