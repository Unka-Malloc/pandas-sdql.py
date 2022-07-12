from pysdql.core.dtypes.RecordExpr import RecExpr


class HavExpr:
    def __init__(self, iter_expr, unit1, op, unit2, groupby_cols):
        self.iter_expr = iter_expr
        self.unit1 = unit1
        self.op = op
        self.unit2 = unit2
        self.groupby_cols = groupby_cols

        self.agg = None

    def sum(self):
        self.agg = 'sum'
        return self

    @property
    def expr(self) -> str:
        return f'{self.unit1} {self.op} {self.unit2}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        return self

    def filter(self, func):
        u1 = self.unit1.new_expr('g_k')
        u2 = self.unit2.new_expr('g_k')

        tmp_dict = {}
        result_dict = {}

        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.iter_expr.key}.{c}'
            result_dict[c] = f'hv_k.{c}'

        agg_str = ''

        if self.agg == 'sum':
            agg_str = f'({u1} {self.op} {u2}) * g_v'

        print(f'let hvmp = {self.iter_expr} sum (<g_k, g_v> in {self.iter_expr.key}.group) {{ {RecExpr(tmp_dict)} -> {agg_str} }} in')

        result_dict['val'] = f'hv_v'

        print(f'let hvR = sum (<hv_k, hv_v> in hvmp) if (hv_v {func}) then {{ {RecExpr(result_dict)} }} else {{ }} in')

    def __gt__(self, other):
        """
        Greater Than ">"
        :param other:
        :return:
        """
        self.filter(f'> {other}')
