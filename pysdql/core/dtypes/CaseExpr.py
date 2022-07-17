from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.ConditionalExpr import CondExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.ConcatExpr import ConcatExpr
from pysdql.core.dtypes.RecordExpr import RecExpr


class CaseExpr:
    def __init__(self, when, then_case, else_case):
        self.when = when
        self.then_case = then_case
        self.else_case = else_case

    def set(self, col_name, r_name, iter_expr):
        next_then_case = DictExpr({ConcatExpr(iter_expr.key, RecExpr({col_name: self.then_case})): 1})
        next_else_case = DictExpr({ConcatExpr(iter_expr.key, RecExpr({col_name: self.else_case})): 1})
        output = VarExpr(r_name, CompoExpr(iter_expr, CondExpr(self.when, next_then_case, next_else_case)))
        return output
        # print(f'let {r_name} = {iter_expr} '
        #       f'if ({self.when}) '
        #       f'then {{ concat({iter_expr.key}, <{col_name}={self.then_case}>) }} '
        #       f'else {{ concat({iter_expr.key}, <{col_name}={self.else_case}>) }} in')
