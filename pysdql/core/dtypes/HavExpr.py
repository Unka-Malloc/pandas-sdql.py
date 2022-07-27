from pysdql.core.dtypes.CondStmt import CondStmt
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.RecEl import RecEl


class HavExpr:
    def __init__(self, groupby_expr, unit1, op, unit2):
        self.unit1 = unit1
        self.op = op
        self.unit2 = unit2
        self.groupby_expr = groupby_expr
        self.groupby_cols = self.groupby_expr.groupby_cols
        self.iter_expr = self.groupby_expr.iter_expr

        self.agg = None

    def sum(self):
        self.agg = 'sum'
        return self

    def new_expr(self, new_str):
        u1 = self.unit1.new_expr(new_str)
        u2 = self.unit2.new_expr(new_str)

        return f'({u1} {self.op} {u2})'

    @property
    def expr(self) -> str:
        return f'{self.unit1} {self.op} {self.unit2}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        return self

    def filter(self, op, other):
        if type(other) == VarExpr:
            self.groupby_expr.inherit(other.inherit_from)

        result_name = 'hvR'
        result_key = 'hv_k'
        result_val = 'hv_v'
        result_cond = CondExpr(f'{result_val}.val', op, other)

        tmp_dict = {}
        result_dict = {}
        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.iter_expr.key}.{c}'
            result_dict[c] = f'{result_key}.{c}'

        tmp_name = 'hvmp'
        tmp_key = 'g_k'
        tmp_val = 'g_v'
        tmp_iter_expr = IterExpr(r_name=f'{self.iter_expr.key}.group', key=tmp_key, val=tmp_val)
        result_iter_expr = IterExpr(tmp_name, result_key, result_val)

        agg_dict = {}
        if self.agg == 'sum':
            agg_dict['val'] = f'({self.new_expr(tmp_key)} * {tmp_val})'

        hvmp = VarExpr(tmp_name, IterStmt([self.iter_expr, tmp_iter_expr],
                                          DictEl({RecEl(tmp_dict):
                                                         RecEl(agg_dict)})))
        self.groupby_expr.history_name.append(tmp_name)
        self.groupby_expr.operations.append(OpExpr('havexpr_hvmp', hvmp))

        hvr = VarExpr(result_name, IterStmt(result_iter_expr,
                                            CondStmt(result_cond, DictEl({RecEl(result_dict): 1}), DictEl({}))))
        self.groupby_expr.history_name.append(result_name)
        self.groupby_expr.operations.append(OpExpr('havexpr_hvr', hvr))

        r_name = self.groupby_expr.groupby_from.name
        r_iter_expr = self.groupby_expr.groupby_from.iter_expr
        r_key = r_iter_expr.key
        r_val = r_iter_expr.val

        new_name = f'fh{r_name}'
        new_iter_expr = IterExpr(result_name)
        new_key = new_iter_expr.key
        new_val = new_iter_expr.val

        new_cond = None
        for i in self.groupby_cols:
            icond = CondExpr(f'{new_key}.{i}', '==', f'{r_key}.{i}')
            if new_cond:
                new_cond &= icond
            else:
                new_cond = icond

        fhvr = VarExpr(new_name,
                       IterStmt([r_iter_expr, new_iter_expr],
                                CondStmt(new_cond, DictEl({r_key: 1}), DictEl({}))))
        self.groupby_expr.history_name.append(new_name)
        self.groupby_expr.operations.append(OpExpr('havexpr_fhvr', fhvr))

        return fhvr

    def __gt__(self, other):
        """
        Greater Than ">"
        :param other:
        :return:
        """
        return self.filter('>', other)
