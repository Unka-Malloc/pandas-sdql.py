from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.ColProjExpr import ColProjExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.MergeExpr import MergeExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VirColEl import VirColEl
from pysdql.core.dtypes.sdql_ir import (
    SumExpr,
    IfExpr,
    VarExpr,
    ConstantExpr, LetExpr, DicConsExpr, RecConsExpr, SumBuilder, ConcatExpr, MulExpr, DicLookupExpr,
)

from pysdql.core.dtypes.EnumUtil import LastFunc, OptGoal


class Optimizer:
    def __init__(self, opt_on, opt_goal=None):
        self.opt_on = opt_on
        self.opt_goal = opt_goal

        self.cond_info = {
            'cond_if': ConstantExpr(None),
            'cond_then': ConstantExpr(None),
            'cond_else': ConstantExpr(None)
        }

        self.cond_status = False

        self.col_ins = {

        }

        self.col_proj = []

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

        self.merge_info = {
            'merge_left': None,
            'merge_right': None,
            'merge_how': 'inner',
            'merge_left_on': ConstantExpr(None),
            'merge_right_on': ConstantExpr(None)
        }

        self.merge_left_info = {
            'merge_left_sum_el': opt_on.iter_el.sdql_ir,
            'merge_left_sum_on': opt_on.var_expr,
            'merge_left_sum_op': ConstantExpr(None),

            'merge_left_let_var': VarExpr(opt_on.name_ops),
            'merge_left_let_val': ConstantExpr(None),
            'merge_left_let_next': ConstantExpr(None)
        }

        self.merge_right_info = {
            'merge_right_sum_el': opt_on.iter_el.sdql_ir,
            'merge_right_sum_on': opt_on.var_expr,
            'merge_right_sum_op': ConstantExpr(None),
            'merge_right_let_var': VarExpr(opt_on.name_ops),
            'merge_right_let_val': ConstantExpr(None),
            'merge_right_let_next': ConstantExpr(None)
        }

        self.status = {
            'conditional': False,
            'column_insertion': False,
            'column_projection': False
        }

    @property
    def has_cond(self):
        return self.status['conditional']

    @property
    def has_col_ins(self):
        return self.status['column_insertion']

    @property
    def has_col_proj(self):
        return self.status['column_projection']

    def add_cond(self, cond):
        if self.cond_info['cond_if'] == ConstantExpr(None):
            self.cond_info['cond_if'] = cond
        else:
            self.cond_info['cond_if'] = MulExpr(self.cond_info['cond_if'], cond)

        self.status['conditional'] = True

    def add_col_ins(self, col_name, col_expr):
        self.col_ins[col_name] = col_expr

        self.status['column_insertion'] = True

    def add_col_proj(self, rec_tuple):
        self.col_proj.append(rec_tuple)

        self.status['column_projection'] = True

    def get_cond_ir(self):
        return self.cond_info['cond_if']

    def get_col_ins_ir(self, col_name: str):
        return self.col_ins[col_name]

    def get_col_proj_ir(self):
        return RecConsExpr(self.col_proj)

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

        rec_dict = {}

        if self.col_ins:
            for k in aggr_dict.keys():
                v = aggr_dict[k]
                if v.name in self.col_ins.keys():
                    col_expr = self.col_ins[v.name].sdql_ir
                else:
                    col_expr = v

                rec_list.append((k, col_expr))

        # if self.col_ins:
        #     for vir_col in self.col_ins.keys():
        #         vir_col_expr = self.col_ins[vir_col]
        #         for k in aggr_dict.keys():
        #             v = aggr_dict[k]
        #             if v.name == vir_col:
        #                 # rec_dict[k] = vir_col_expr.sdql_ir
        #                 # rec_list.append((k, vir_col_expr.sdql_ir))
        #             else:
        #                 # rec_dict[k] = v
        #                 # rec_list.append((k, v))

        # for i in rec_dict.keys():
        #     rec_list.append((i, rec_dict[i]))

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
            self.add_cond(op_expr.op.sdql_ir)

            self.cond_info['cond_if'] = op_expr.op.sdql_ir
            self.cond_status = True
        if op_expr.op_type == AggrExpr:
            self.cond_info['cond_then'] = op_expr.op.aggr_op
            self.cond_info['cond_else'] = op_expr.op.aggr_else
            self.sum_info['sum_op'] = self.cond_stmt

            self.last_func = LastFunc.Agg
        if op_expr.op_type == VirColEl:
            self.add_col_ins(col_name=op_expr.op.col_var,
                             col_expr=op_expr.op.col_expr)

            self.col_ins[op_expr.op.col_var] = op_expr.op.col_expr
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

        if op_expr.op_type == ColProjExpr:
            for col in op_expr.op.proj_cols:
                self.add_col_proj((col,
                                   op_expr.op.proj_on.key_access(col)))

        if op_expr.op_type == MergeExpr:
            self.merge_info['left'] = op_expr.op.left
            self.merge_info['right'] = op_expr.op.right
            self.merge_info['left_on'] = op_expr.op.left_on
            self.merge_info['right_on'] = op_expr.op.right_on
            self.merge_info['how'] = op_expr.op.how

    def merge_left_stmt(self, merge_right_stmt):
        if self.has_cond:
            part_left_op = IfExpr(condExpr=self.get_cond_ir(),
                                  thenBodyExpr=DicConsExpr([(
                                      RecConsExpr([(self.merge_info['left_on'],
                                                    self.opt_on.key_access(self.merge_info['left_on']))]),
                                      self.get_col_proj_ir()
                                  )]),
                                  elseBodyExpr=ConstantExpr(None))
        else:
            part_left_op = DicConsExpr([(
                self.opt_on.key_access(self.merge_info['left_on']),
                self.get_col_proj_ir()
            )])

        self.merge_left_info['merge_left_sum_op'] = part_left_op

        part_left_sum = SumExpr(varExpr=self.merge_left_info['merge_left_sum_el'],
                                dictExpr=self.merge_left_info['merge_left_sum_on'],
                                bodyExpr=self.merge_left_info['merge_left_sum_op'])

        self.merge_left_info['merge_left_let_val'] = part_left_sum
        self.merge_left_info['merge_left_let_next'] = merge_right_stmt

        return LetExpr(varExpr=self.merge_left_info['merge_left_let_var'],
                       valExpr=self.merge_left_info['merge_left_let_val'],
                       bodyExpr=self.merge_left_info['merge_left_let_next'])

    def merge_right_stmt(self, merge_next_stmt):
        merge_left_opt = self.merge_info['left'].get_opt(OptGoal.MergeLeftPart)
        merge_left_var = merge_left_opt.merge_left_info['merge_left_let_var']

        if self.has_cond:
            right_op = IfExpr(condExpr=self.get_cond_ir(),
                              thenBodyExpr=IfExpr(condExpr=DicLookupExpr(dicExpr=merge_left_var,
                                                                         keyExpr=self.opt_on.key_access(
                                                                             self.merge_info['right_on']))
                                                           != ConstantExpr(None),
                                                  thenBodyExpr=DicConsExpr([(
                                                      RecConsExpr([(self.merge_info['right_on'],
                                                                    self.opt_on.key_access(
                                                                        self.merge_info['right_on']))]),
                                                      self.get_col_proj_ir()
                                                  )]),
                                                  elseBodyExpr=ConstantExpr(None)),
                              elseBodyExpr=ConstantExpr(None))
        else:
            right_op = IfExpr(condExpr=DicLookupExpr(dicExpr=merge_left_var,
                                                     keyExpr=self.opt_on.key_access(self.merge_info['right_on']))
                                       != ConstantExpr(None),
                              thenBodyExpr=DicConsExpr([(
                                  RecConsExpr([(self.merge_info['right_on'],
                                                self.opt_on.key_access(self.merge_info['right_on']))]),
                                  self.get_col_proj_ir()
                              )]),
                              elseBodyExpr=ConstantExpr(None))

        self.merge_right_info['merge_right_sum_op'] = right_op

        right_sum = SumExpr(varExpr=self.merge_right_info['merge_right_sum_el'],
                            dictExpr=self.merge_right_info['merge_right_sum_on'],
                            bodyExpr=self.merge_right_info['merge_right_sum_op'])

        self.merge_right_info['merge_right_let_val'] = right_sum
        self.merge_right_info['merge_right_let_next'] = merge_next_stmt

        return LetExpr(varExpr=self.merge_right_info['merge_right_let_var'],
                       valExpr=self.merge_right_info['merge_right_let_val'],
                       bodyExpr=self.merge_right_info['merge_right_let_next'])

    @property
    def info(self):
        return {
            'cond': self.get_cond_ir(),
            'col_proj': self.get_col_proj_ir(),
        }

    @property
    def output(self):
        if self.last_func == LastFunc.Agg:
            result = VarExpr('result')
            return LetExpr(result,
                           self.sum_stmt,
                           LetExpr(VarExpr('out'),
                                   result,
                                   ConstantExpr(True)))
        if self.last_func == LastFunc.GroupbyAgg:
            return self.groupby_aggr_stmt
        else:
            raise ValueError()
