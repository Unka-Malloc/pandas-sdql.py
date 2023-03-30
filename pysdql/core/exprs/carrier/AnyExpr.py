class AnyExpr:
    def __init__(self, *args):
        self.args = args

    @property
    def expr(self):
        return ' '.join([str(i) for i in self.args])

    def __repr__(self):
        return self.expr
