from pysdql.core.dtypes.DictIR import DictIR
from pysdql.core.dtypes.RecIR import RecIR


class SumOpt:
    def __init__(self, sum_on):
        self.sum_on = sum_on
        self.sum_func = None
        self.sum_if = None
        self.sum_else = None
        self.sum_update = True

        self.info = {
            'sum_on': self.sum_on,
            'sum_func': self.sum_func,
            'sum_if': self.sum_if,
            'sum_else': self.sum_else,
            'sum_update': self.sum_update
        }

        self.var_cols = {}

        self.is_groupby_agg = False
        self.groupby_agg_expr = None

    @property
    def expr(self):
        if self.is_groupby_agg:
            return self.groupby_agg_expr
        return f'{self.sum_on.name}.sum(lambda p: {self.sum_func} if {self.sum_if} else {self.sum_else}, {self.sum_update})'

    def __str__(self):
        return self.expr

    def add_cond(self, cond_if):
        if self.sum_if:
            self.sum_if = f'{self.sum_if} and {cond_if}'
        else:
            self.sum_if = f'{cond_if}'

    def merge(self, sum_expr):
        if self.sum_on.name == sum_expr.sum_on.name:

            if sum_expr.sum_func:
                if self.sum_func:
                    pass
                else:
                    self.sum_func = sum_expr.sum_func

            if sum_expr.sum_if:
                self.add_cond(sum_expr.sum_if)

            if sum_expr.sum_else is not None:
                if self.sum_else is not None:
                    pass
                else:
                    self.sum_else = sum_expr.sum_else

    def groupby_agg(self, groupby_agg_expr):
        self.is_groupby_agg = True

        if self.sum_on.name == groupby_agg_expr.groupby_from.name:
            groupby_keys = {}

            for col in groupby_agg_expr.groupby_cols:
                groupby_keys[col] = f'{groupby_agg_expr.groupby_from.key}.{col}'

            groupby_agg_val = groupby_agg_expr.agg_dict

            if self.var_cols:
                for col in self.var_cols:
                    for k in groupby_agg_expr.agg_dict.keys():
                        v = groupby_agg_expr.agg_dict[k]
                        if f'{groupby_agg_expr.groupby_from.key}.{col}' == v:
                            groupby_agg_val[k] = self.var_cols[col]
                        else:
                            groupby_agg_val[k] = v

            self.sum_func = DictIR({RecIR(groupby_keys): RecIR(groupby_agg_val)})

            self.groupby_agg_expr = f'{self.sum_on.name}.sum(lambda p: {self.sum_func} if {self.sum_if} else {self.sum_else}, {self.sum_update})' \
                                    f'.sum(lambda p: {{p[0].concat(p[1]): True}}, {self.sum_update})'

