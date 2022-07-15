from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.HavingExpr import HavExpr


class HavUnit:
    def __init__(self, iter_expr, groupby_expr, col_name=None):
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

    def filter(self, op, other):
        tmp_dict = {}
        result_dict = {}

        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.iter_expr.key}.{c}'
            result_dict[c] = f'hv_k.{c}'

        agg_str = ''

        if self.agg == 'sum':
            agg_str = f'{self.col_name} * g_v'

        tmp_name = 'hvmp'
        tmp_iter_key = 'g_k'
        tmp_iter_val = 'g_v'
        tmp_iter_expr = f'sum (<{tmp_iter_key}, {tmp_iter_val}> in {self.iter_expr.key}.group'

        hvmp = VarExpr(tmp_name,
                       CompoExpr([self.iter_expr, tmp_iter_expr],
                                 DictExpr({RecExpr(tmp_dict): f'{tmp_iter_key}.{agg_str}'})))

        print(hvmp)

        result_dict['val'] = f'hv_v'

        print(f'let hvR = sum (<hv_k, hv_v> in hvmp) if (hv_v {op}) then {{ {RecExpr(result_dict)} }} else {{ }} in')

    @property
    def expr(self):
        return f'{self.iter_expr.key}.{self.col_name}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        if type(item) == str:
            return HavUnit(iter_expr=self.iter_expr,
                           groupby_expr=self.grouby_expr,
                           col_name=item)

    def __mul__(self, other):
        return HavExpr(iter_expr=self.iter_expr,
                       groupby_expr=self.grouby_expr,
                       unit1=self,
                       op='*',
                       unit2=other)

    def __gt__(self, other):
        """
        Greater Than ">"
        :param other:
        :return:
        """
        self.filter(op='>', other=other)
