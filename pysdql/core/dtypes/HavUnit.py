from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.OpExpr import OpExpr


class HavUnit:
    def __init__(self, iter_expr, col_name=None, groupby_expr=None):
        self.grouby_expr = groupby_expr
        self.groupby_cols = self.grouby_expr.groupby_cols
        self.iter_expr = iter_expr
        self.col_name = col_name

        self.agg = None

    def new_expr(self, new_str) -> str:
        return f'{new_str}.{self.col_name}'

    def sum(self):
        self.agg = 'sum'
        return self

    def filter(self, func):
        tmp_dict = {}
        result_dict = {}

        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.iter_expr.key}.{c}'
            result_dict[c] = f'hv_k.{c}'

        agg_str = ''

        if self.agg == 'sum':
            agg_str = f'{self.col_name} * g_v'

        print(f'let hvmp = {self.iter_expr} sum (<g_k, g_v> in {self.iter_expr.key}.group) {{ {RecExpr(tmp_dict)} -> g_k.{agg_str} }} in')

        result_dict['val'] = f'hv_v'

        print(f'let hvR = sum (<hv_k, hv_v> in hvmp) if (hv_v {func}) then {{ {RecExpr(result_dict)} }} else {{ }} in')

    @property
    def expr(self):
        return f'{self.iter_expr.key}.{self.col_name}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        if type(item) == str:
            return HavUnit(self.iter_expr, item, self.groupby_cols)

    def __mul__(self, other):
        from pysdql.core.dtypes.HavingExpr import HavExpr
        return HavExpr(self.iter_expr, self, '*', other, self.groupby_cols)

    def __gt__(self, other):
        """
        Greater Than ">"
        :param other:
        :return:
        """
        self.filter(f'> {other}')