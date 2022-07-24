from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.CondStmt import CondStmt
from pysdql.core.dtypes.DictExpr import DictExpr
from pysdql.core.dtypes.ConcatExpr import ConcatExpr
from pysdql.core.dtypes.RecordExpr import RecExpr


class CaseExpr:
    def __init__(self, r, when, then_case, else_case):
        self.r = r
        self.when = when
        self.then_case = then_case
        self.else_case = else_case

    @property
    def expr(self):
        return f'(if ({self.when}) then {self.then_case} else {self.else_case})'

    def __repr__(self):
        return self.expr

    def set(self, col_name, next_name, iter_expr):
        trec = RecExpr({col_name: self.then_case})
        erec = RecExpr({col_name: self.else_case})

        next_then_case = DictExpr({f'concat({iter_expr.key}, {trec})': 1})
        next_else_case = DictExpr({f'concat({iter_expr.key}, {erec})': 1})

        output = VarExpr(next_name, IterStmt(iter_expr, CondStmt(conditions=self.when,
                                                                 then_case=next_then_case,
                                                                 else_case=next_else_case,
                                                                 new_iter=iter_expr.key)))

        return output
