from pysdql.core.dtypes.OpExpr import OpExpr


class OpSeq:
    def __init__(self):
        self.stack = []
        self.names = []

    def pop(self):
        self.stack.pop()

    def push(self, val):
        if isinstance(val, OpExpr):
            self.stack.append(val)
            self.names.append(val.name)
        else:
            raise ValueError(f'Only accept OpExpr.')

    def peek(self):
        if len(self.stack) >= 1:
            return self.stack[-1]
        return

    @property
    def size(self):
        return len(self.stack)

    @property
    def expr(self):
        expr_str = ''
        if len(self.stack) >= 1:
            expr_str = f'\n'.join([f'{i}' for i in self.stack])
        return expr_str

    def __str__(self):
        return self.expr

    def __repr__(self):
        return self.expr

    def __iter__(self):
        return iter(self.stack)

    def __reversed__(self):
        return reversed(self.stack)

    def __getitem__(self, item):
        return self.stack[item]


