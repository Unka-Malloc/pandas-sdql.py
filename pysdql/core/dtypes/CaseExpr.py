from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.ConditionalExpr import CondExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
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
        return f'{self.when}, {self.then_case}, {self.else_case}'

    def __repr__(self):
        return self.expr

    def set(self, col_name, next_name, iter_expr):
        trec = RecExpr({col_name: self.then_case})
        erec = RecExpr({col_name: self.else_case})

        next_then_case = DictExpr({f'concat({iter_expr.key}, {trec})': 1})
        next_else_case = DictExpr({f'concat({iter_expr.key}, {erec})': 1})

        output = VarExpr(next_name, CompoExpr(iter_expr, CondExpr(conditions=self.when,
                                                                  then_case=next_then_case,
                                                                  else_case=next_else_case,
                                                                  new_iter=iter_expr.key)))

        return output

        # print(f'let {r_name} = {iter_expr} '
        #       f'if ({self.when}) '
        #       f'then {{ concat({iter_expr.key}, <{col_name}={self.then_case}>) }} '
        #       f'else {{ concat({iter_expr.key}, <{col_name}={self.else_case}>) }} in')
