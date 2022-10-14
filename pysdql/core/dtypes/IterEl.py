from pysdql.core.dtypes.SDQLIR import SDQLIR
from pysdql.core.dtypes.sdql_ir import (
    PairAccessExpr,
    VarExpr
)


class IterEl(SDQLIR):
    def __init__(self, name: str):
        self.__name = name
        self.__el = VarExpr(name)

    @property
    def name(self):
        return self.__name

    @property
    def el(self):
        return self.__el

    @property
    def key(self):
        return PairAccessExpr(self.el, 0)

    @property
    def k(self):
        return self.key

    @property
    def value(self):
        return PairAccessExpr(self.el, 1)

    @property
    def val(self):
        return self.value

    @property
    def v(self):
        return self.value

    @property
    def sdql_ir(self):
        return self.el
