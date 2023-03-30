from pysdql.core.interfaces.availability.Replaceable import Replaceable
from pysdql.core.prototype.basic.sdql_ir import (
    PairAccessExpr,
    VarExpr
)


class IterEl(Replaceable):
    def __init__(self, name: str):
        self.__name = name
        self.__el = VarExpr(name)  # x = VarExpr('x')

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

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return hash((
            self.name
        ))

    @property
    def sdql_ir(self):
        return self.el
