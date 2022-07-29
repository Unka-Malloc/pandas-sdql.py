from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.CondStmt import CondStmt
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.ConcatExpr import ConcatExpr
from pysdql.core.dtypes.RecEl import RecEl


class CaseExpr:
    def __init__(self, when, then_case, else_case):
        self.when = when
        self.then_case = then_case
        self.else_case = else_case

    @property
    def expr(self):
        return f'(if ({self.when}) then {self.then_case} else {self.else_case})'

    def __repr__(self):
        return self.expr

    def set(self, col_name, next_name, iter_expr):
        cond = self.when
        if type(cond) == ExternalExpr:
            cond = cond.new_expr(iter_expr.key)

        trec = RecEl({col_name: self.then_case})
        erec = RecEl({col_name: self.else_case})

        next_then_case = DictEl({f'concat({iter_expr.key}, {trec})': 1})
        next_else_case = DictEl({f'concat({iter_expr.key}, {erec})': 1})

        output = VarExpr(next_name, IterStmt(iter_expr, CondStmt(conditions=cond,
                                                                 then_case=next_then_case,
                                                                 else_case=next_else_case,
                                                                 new_iter=iter_expr.key)))

        return output
