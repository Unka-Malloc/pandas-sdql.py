from pysdql.core.dtypes.VarBindExpr import VarBindExpr
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.sdql_ir import (
    VarExpr,
    ConstantExpr,
)


class VarBindSeq(FlexIR):
    def __init__(self):
        self.__seq = []

    @property
    def seq(self):
        return self.__seq

    def push(self, val, *args):
        if type(val) == VarBindExpr:
            self.__seq.append(val)
            if args:
                for var in args:
                    self.__seq.append(var)
            return
        if type(val) in (tuple, list):
            for var in val:
                self.__seq.append(var)
            return
        raise ValueError()

    def pop(self):
        result = self.__seq[-1]
        del self.__seq[-1]
        return result

    def peek(self):
        return self.__seq[-1]

    def get_sdql_ir(self, last_binding):
        if self.seq:
            result = self.pop()
            result = result.concat(last_binding)
            for binding in reversed(self.seq):
                result = result.fillin(binding)
            return result.sdql_ir
        else:
            return last_binding

    def __repr__(self):
        result = ''
        for expr in self.seq:
            result += f'{expr}\n'
        result += f'{self.peek().next_expr}'
        return result

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return True

    @property
    def sdql_ir(self):
        if not self.peek().iscompleted:
            result = self.pop()
            last_binding = VarBindExpr(var_expr=VarExpr('out'),
                                       var_value=result.var_expr,
                                       next_expr=ConstantExpr(True))
            result = result.concat(last_binding)
            for binding in reversed(self.seq):
                result = result.fillin(binding)
            return result.sdql_ir
        else:
            raise ValueError()
