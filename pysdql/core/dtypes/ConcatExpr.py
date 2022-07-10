class ConcatExpr:
    def __init__(self, rec1, rec2):
        self.rec1 = rec1
        self.rec2 = rec2

    @property
    def expr(self) -> str:
        return f'concat({self.rec1}, {self.rec2})'

    def __repr__(self):
        return self.expr

    def concat(self, other):
        return ConcatExpr(self, other)

    @staticmethod
    def concat_list(keys):
        k1 = keys.pop()
        k2 = keys.pop()
        ce = ConcatExpr(rec1=k1, rec2=k2)
        for i in keys:
            ce = ce.concat(i)


