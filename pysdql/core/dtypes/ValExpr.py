class ValExpr:
    def __init__(self, name: str, operations=None):
        self.name = name
        self.operations = operations

    @property
    def expr(self) -> str:
        return self.name

    def __repr__(self):
        return self.expr

    @property
    def sdql_expr(self) -> str:
        if self.operations:
            return self.operations.expr
        else:
            return self.name

    def __str__(self):
        return self.sdql_expr


