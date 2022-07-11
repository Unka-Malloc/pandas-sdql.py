class VarExpr:
    def __init__(self, name, data=None):
        self.name = name
        self.data = data

    def rename(self, new_name):
        self.name = new_name

    @property
    def expr(self) -> str:
        return self.name

    def __repr__(self):
        return self.expr
