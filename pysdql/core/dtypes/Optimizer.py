from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VirColEl import VirColEl
from pysdql.core.dtypes.sdql_ir import (
    Expr,
    SumExpr,
    IfExpr,
    VarExpr,
    ConstantExpr, LetExpr, DicConsExpr, RecConsExpr, SumBuilder, ConcatExpr,
)

from pysdql.core.dtypes.EnumUtil import LastFunc


class Optimizer:
    def __init__(self, opt_on):
        self.vir_cols = {

        }

        self.cond_info = {
            'cond_if': ConstantExpr(None),
            'cond_then': ConstantExpr(None),
            'cond_else': ConstantExpr(None)
        }

        self.cond_status = False

        self.sum_info = {
            'sum_el': opt_on.iter_el.sdql_ir,
            'sum_on': opt_on.var_expr,
            'sum_op': ConstantExpr(None),
        }

        self.groupby_aggr_info = {
            'aggr_keys': RecConsExpr([]),
            'aggr_vals': RecConsExpr([]),

            'aggr_var': VarExpr(f'{opt_on.name}_groupby_agg'),
            'aggr_el': opt_on.iter_el.sdql_ir,
            'aggr_on': opt_on.var_expr,
            'aggr_op': ConstantExpr(None),

            'let_var': VarExpr(opt_on.name_ops),
            'let_val': ConstantExpr(None),
            'let_next': ConstantExpr(None),
        }

        self.last_func = None

    @property
    def cond_stmt(self):
        return IfExpr(condExpr=self.cond_info['cond_if'],
                      thenBodyExpr=self.cond_info['cond_then'],
                      elseBodyExpr=self.cond_info['cond_else'])

    @property
    def sum_stmt(self):
        return SumExpr(varExpr=self.sum_info['sum_el'],
                       dictExpr=self.sum_info['sum_on'],
                       bodyExpr=self.sum_info['sum_op'])

    @property
    def let_stmt(self):
        return LetExpr(varExpr=self.groupby_aggr_info['let_var'],
                       valExpr=self.groupby_aggr_info['let_val'],
                       bodyExpr=self.groupby_aggr_info['let_next'])

    def set_groupby_aggr_key_part(self, on, cols):
        self.groupby_aggr_info['aggr_keys'] = RecConsExpr([(i, on.key_access(i)) for i in cols])

    def set_groupby_aggr_val_part(self, aggr_dict):
        rec_list = []

        if self.vir_cols:
            for vir_col in self.vir_cols.keys():
                vir_col_expr = self.vir_cols[vir_col]
                for k in aggr_dict.keys():
                    v = aggr_dict[k]
                    if v.name == vir_col:
                        rec_list.append((k, vir_col_expr.sdql_ir))
                    else:
                        rec_list.append((k, v))

        self.groupby_aggr_info['aggr_vals'] = RecConsExpr(rec_list)

    def set_groupby_aggr_aggr_op(self):
        self.groupby_aggr_info['aggr_op'] = DicConsExpr([(self.groupby_aggr_info['aggr_keys'],
                                                          self.groupby_aggr_info['aggr_vals'])])

    def set_groupby_aggr_let_val(self):
        if self.cond_status:
            self.groupby_aggr_info['let_val'] = SumExpr(varExpr=self.groupby_aggr_info['aggr_el'],
                                                        dictExpr=self.groupby_aggr_info['aggr_on'],
                                                        bodyExpr=IfExpr(condExpr=self.cond_info['cond_if'],
                                                                        thenBodyExpr=self.cond_info['cond_then'],
                                                                        elseBodyExpr=self.cond_info['cond_else']))
        else:
            self.groupby_aggr_info['let_val'] = SumExpr(varExpr=self.groupby_aggr_info['aggr_el'],
                                                        dictExpr=self.groupby_aggr_info['aggr_on'],
                                                        bodyExpr=self.groupby_aggr_info['aggr_op'])

    @property
    def groupby_aggr_stmt(self):
        return LetExpr(varExpr=self.groupby_aggr_info['aggr_var'],
                       valExpr=self.groupby_aggr_info['let_val'],
                       bodyExpr=self.groupby_aggr_info['let_next'])

    def input(self, op_expr: OpExpr):
        if op_expr.op_type == CondExpr:
            self.cond_info['cond_if'] = op_expr.op.sdql_ir

            self.cond_status = True
        if op_expr.op_type == AggrExpr:
            self.cond_info['cond_then'] = op_expr.op.aggr_op
            self.cond_info['cond_else'] = op_expr.op.aggr_else
            self.sum_info['sum_op'] = self.cond_stmt

            self.last_func = LastFunc.Agg
        if op_expr.op_type == VirColEl:
            self.vir_cols[op_expr.op.col_var] = op_expr.op.col_expr
        if op_expr.op_type == GroupByAgg:
            groupby_from = op_expr.op.groupby_from
            groupby_cols = op_expr.op.groupby_cols
            aggr_dict = op_expr.op.agg_dict

            self.set_groupby_aggr_key_part(groupby_from, groupby_cols)
            self.set_groupby_aggr_val_part(aggr_dict)

            self.set_groupby_aggr_aggr_op()

            self.cond_info['cond_then'] = self.groupby_aggr_info['aggr_op']
            self.cond_info['cond_else'] = ConstantExpr(None)

            self.set_groupby_aggr_let_val()

            self.groupby_aggr_info['let_next'] = LetExpr(varExpr=self.groupby_aggr_info['let_var'],
                                                         valExpr=SumBuilder(lambda p:
                                                                            DicConsExpr([(ConcatExpr(p[0], p[1]),
                                                                                          ConstantExpr(True))]),
                                                                            self.groupby_aggr_info['aggr_var'],
                                                                            True),
                                                         bodyExpr=LetExpr(VarExpr("out"),
                                                                          self.groupby_aggr_info['let_var'],
                                                                          ConstantExpr(True)))

            self.last_func = LastFunc.GroupbyAgg

    @property
    def output(self):
        if self.last_func == LastFunc.Agg:
            return self.sum_stmt
        if self.last_func == LastFunc.GroupbyAgg:
            return self.groupby_aggr_stmt
        else:
            raise ValueError()
