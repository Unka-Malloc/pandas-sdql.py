from pysdql.core.dtypes.OpExpr import OpExpr


class OpSeq:
    def __init__(self):
        self.stack = []
        self.names = []

    def pop(self):
        self.stack.pop()

    def push(self, val):
        if type(val) == OpExpr:
            self.stack.append(val)
            self.names.append(val.name)
        else:
            raise ValueError(f'Only accept OpExpr.')

    def peak(self):
        return self.stack[-1]

    @property
    def size(self):
        return len(self.stack)

    @property
    def expr(self):
        expr_str = f'\n'.join([f'{i}' for i in self.stack])
        expr_str += f'\n{self.peak().name}'
        return expr_str

    def __repr__(self):
        return self.expr
