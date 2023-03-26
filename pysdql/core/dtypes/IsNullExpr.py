from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr


class IsNullExpr(IgnoreExpr):
    def __init__(self, col):
        self.col = col
        self.is_invert = False

    def __invert__(self):
        self.is_invert = True

        return self