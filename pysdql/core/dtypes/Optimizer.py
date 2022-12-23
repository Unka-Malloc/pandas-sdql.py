from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.ColProjExpr import ColProjExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.JointFrame import JointFrame
from pysdql.core.dtypes.JoinPartitionFrame import JoinPartitionFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VirColEl import VirColEl
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.sdql_ir import *

from pysdql.core.dtypes.EnumUtil import LastIterFunc, OptGoal, MergeType, OperationReturnType


class Optimizer:
    def __init__(self, opt_on, opt_goal=None):
        self.opt_on = opt_on
        self.opt_goal = opt_goal

        self.cond_info = {
            'cond_if': ConstantExpr(True),
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

        self.agg_dict_info = {
            'aggr_dict': None,
            'cond_if': ConstantExpr(None),
            'cond_then': ConstantExpr(None),
            'cond_else': ConstantExpr(None),

            'sum_el': opt_on.iter_el.sdql_ir,
            'sum_on': opt_on.var_expr,
            'sum_op': ConstantExpr(None),
        }

        self.vname_groupby_agg = f'{opt_on.name}_groupby_agg'
        self.var_groupby_agg = VarExpr(self.vname_groupby_agg)
        # self.opt_on.add_context_variable(self.vname_groupby_agg, self.var_groupby_agg)

        self.vname_groupby_agg_concat = f'{opt_on.name}_groupby_agg_concat'
        self.var_groupby_agg_concat = VarExpr(self.vname_groupby_agg_concat)
        # self.opt_on.add_context_variable(self.vname_groupby_agg_concat, self.var_groupby_agg_concat)

        self.groupby_aggr_info = {
            'groupby_cols': [],
            'aggr_dict': {},

            'aggr_keys': RecConsExpr([]),
            'aggr_vals': RecConsExpr([]),

            'aggr_var': self.var_groupby_agg,
            'aggr_el': opt_on.iter_el.sdql_ir,
            'aggr_on': opt_on.var_expr,
            'aggr_op': ConstantExpr(None),

            'let_var': self.var_groupby_agg_concat,
            'let_val': ConstantExpr(None),
            'let_next': ConstantExpr(None),
        }

        self.last_func = None

        self.last_merge_info = {
            'merge_left': None,
            'merge_right': None,
            'merge_how': 'inner',
            'merge_left_on': ConstantExpr(None),
            'merge_right_on': ConstantExpr(None)
        }

        self.merge_left_info = {
            'merge_left_on': '',

            'merge_left_sum_el': opt_on.iter_el.sdql_ir,
            'merge_left_sum_on': opt_on.var_expr,
            'merge_left_sum_op': ConstantExpr(None),

            'merge_left_let_var': self.opt_on.part_var,
            'merge_left_let_val': ConstantExpr(None),
            'merge_left_let_next': ConstantExpr(None)
        }

        self.merge_right_info = {
            'merge_right_on': '',

            'merge_right_sum_el': opt_on.iter_el.sdql_ir,
            'merge_right_sum_on': opt_on.var_expr,
            'merge_right_sum_op': ConstantExpr(None),

            'merge_right_let_var': self.opt_on.var_probe,
            'merge_right_let_val': ConstantExpr(None),
            'merge_right_let_next': ConstantExpr(None)
        }

        self.merge_join_frame_info = {
            'partition_side': None,
            'probe_side': None,
        }

        self.join_partition_info = {
            'partition_key': None,
        }

        self.join_probe_info = {
            'probe_key': None,
        }

        self.joint_info = {
            'partition_side': None,
            'partition_key': None,

            'probe_side': None,
            'probe_key': None,

            'how': None,
            'joint_cond': None
        }

        self.is_join_partition_side = False
        self.is_join_probe_side = False
        self.is_joint = self.opt_on.is_joint

        self.status = {
            'conditional': False,
            'column_insertion': False,
            'column_projection': False
        }

        self.isin_op = None
        self.has_isin = False

        self.vname_having = f'{self.opt_on.name}_having'
        self.var_having = VarExpr(self.vname_having)
        self.opt_on.add_context_variable(self.vname_having, self.var_having)

    @property
    def has_cond(self):
        for op_expr in self.opt_on.operations:
            if op_expr.op_type == CondExpr:
                return True
        return False

    def get_cond_after_groupby_agg(self):
        groupby_agg_located = False
        for op_expr in self.opt_on.operations:
            if op_expr.op_type == GroupByAgg:
                groupby_agg_located = True
            if op_expr.op_type == CondExpr:
                if groupby_agg_located:
                    return op_expr.op
        return None

    @property
    def has_col_ins(self):
        return self.status['column_insertion']

    @property
    def has_col_proj(self):
        return self.status['column_projection']

    def add_cond(self, cond):
        if self.cond_info['cond_if']:
            self.cond_info['cond_if'] = MulExpr(self.cond_info['cond_if'], cond)
        else:
            self.cond_info['cond_if'] = cond

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

    def get_col_proj_ir(self, merge_type: MergeType) -> RecConsExpr:
        if merge_type == MergeType.NONE:
            return RecConsExpr(self.col_proj)
        if merge_type == MergeType.PARTITION:
            if self.col_proj:
                return RecConsExpr(self.col_proj)
            else:
                left_on = self.last_merge_info['left_on']
                return RecConsExpr([(left_on,
                                     self.opt_on.key_access(left_on))])
        if merge_type == MergeType.PROBE:
            if self.col_proj:
                return RecConsExpr(self.col_proj)
            else:
                right_on = self.last_merge_info['right_on']
                return RecConsExpr([(right_on,
                                     self.opt_on.iter_el.key)])
                # right_on = self.merge_info['right_on']
                # return RecConsExpr([(right_on,
                #                      self.opt_on.key_access(right_on))])

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
        if len(cols) == 1:
            self.groupby_aggr_info['aggr_keys'] = on.key_access(cols[0])
        else:
            self.groupby_aggr_info['aggr_keys'] = RecConsExpr([(i, on.key_access(i)) for i in cols])

    def set_groupby_aggr_val_part(self, aggr_dict):
        rec_list = []

        if self.col_ins:
            for k in aggr_dict.keys():
                v = aggr_dict[k]
                if v.name in self.col_ins.keys():
                    col_expr = self.col_ins[v.name].sdql_ir
                else:
                    col_expr = v

                rec_list.append((k, col_expr))
        else:
            for k in aggr_dict.keys():
                rec_list.append((k, aggr_dict[k]))

        self.groupby_aggr_info['aggr_vals'] = RecConsExpr(rec_list)

    def set_groupby_aggr_aggr_op(self):
        self.groupby_aggr_info['aggr_op'] = DicConsExpr([(self.groupby_aggr_info['aggr_keys'],
                                                          self.groupby_aggr_info['aggr_vals'])])

    def set_groupby_aggr_let_val(self):
        if self.has_cond:

            # cond_after_groupby_agg = self.get_cond_after_groupby_agg()
            # if cond_after_groupby_agg:
            #     vname_groupby_agg_el = f'x_{self.vname_groupby_agg}'
            #     var_groupby_agg_el = VarExpr(vname_groupby_agg_el)
            #     self.opt_on.add_context_variable(vname_groupby_agg_el, var_groupby_agg_el)
            #     vname_having = f'having_{self.opt_on.name}'
            #     var_having = VarExpr(vname_having)
            #     self.opt_on.add_context_variable(vname_having, var_having)
            #     next_op = LetExpr(var_having,
            #                       SumExpr(varExpr=var_groupby_agg_el,
            #                               dictExpr=self.var_groupby_agg,
            #                               bodyExpr=IfExpr(condExpr=cond_after_groupby_agg.replace(
            #                                   PairAccessExpr(var_groupby_agg_el, 1)),
            #                                   thenBodyExpr=DicConsExpr(
            #                                       [(PairAccessExpr(var_groupby_agg_el, 0),
            #                                         ConstantExpr(True))]),
            #                                   elseBodyExpr=EmptyDicConsExpr())),
            #                       ConstantExpr(True))

            if self.get_cond_after_groupby_agg():
                self.groupby_aggr_info['let_val'] = SumExpr(varExpr=self.groupby_aggr_info['aggr_el'],
                                                            dictExpr=self.groupby_aggr_info['aggr_on'],
                                                            bodyExpr=self.groupby_aggr_info['aggr_op'])
            else:
                self.groupby_aggr_info['let_val'] = SumExpr(varExpr=self.groupby_aggr_info['aggr_el'],
                                                            dictExpr=self.groupby_aggr_info['aggr_on'],
                                                            bodyExpr=IfExpr(condExpr=self.cond_info['cond_if'],
                                                                            thenBodyExpr=self.cond_info['cond_then'],
                                                                            elseBodyExpr=self.cond_info['cond_else']))
        else:
            self.groupby_aggr_info['let_val'] = SumExpr(varExpr=self.groupby_aggr_info['aggr_el'],
                                                        dictExpr=self.groupby_aggr_info['aggr_on'],
                                                        bodyExpr=self.groupby_aggr_info['aggr_op'])

    def get_groupby_aggr_stmt(self, next_op=None):
        self.opt_on.add_context_variable(self.vname_groupby_agg, self.var_groupby_agg)
        self.opt_on.add_context_variable(self.vname_groupby_agg_concat, self.var_groupby_agg_concat)

        if next_op is None:
            return LetExpr(varExpr=self.groupby_aggr_info['aggr_var'],
                           valExpr=self.groupby_aggr_info['let_val'],
                           bodyExpr=self.groupby_aggr_info['let_next'])
        else:
            return LetExpr(varExpr=self.groupby_aggr_info['aggr_var'],
                           valExpr=self.groupby_aggr_info['let_val'],
                           bodyExpr=next_op)

    def get_groupby_agg_having_stmt(self, next_op=None):
        if next_op is None:
            next_op = ConstantExpr(True)

        cond_after_groupby_agg = self.get_cond_after_groupby_agg()
        if cond_after_groupby_agg:
            vname_groupby_agg_el = f'x_{self.vname_groupby_agg}'
            var_groupby_agg_el = VarExpr(vname_groupby_agg_el)
            self.opt_on.add_context_variable(vname_groupby_agg_el, var_groupby_agg_el)
            vname_having = f'{self.opt_on.name}_having'
            var_having = VarExpr(vname_having)
            self.opt_on.add_context_variable(vname_having, var_having)
            having_op = LetExpr(var_having,
                                SumExpr(varExpr=var_groupby_agg_el,
                                        dictExpr=self.var_groupby_agg,
                                        bodyExpr=IfExpr(condExpr=cond_after_groupby_agg.replace(
                                            PairAccessExpr(var_groupby_agg_el, 1)),
                                            thenBodyExpr=DicConsExpr(
                                                [(PairAccessExpr(var_groupby_agg_el, 0),
                                                  ConstantExpr(True))]),
                                            elseBodyExpr=EmptyDicConsExpr()),
                                        isAssignmentSum=True),
                                next_op)
            return self.get_groupby_aggr_stmt(having_op)
        else:
            raise NotImplementedError

    @property
    def groupby_aggr_stmt(self) -> LetExpr:
        return self.get_groupby_aggr_stmt()

    def input(self, op_expr: OpExpr):
        if op_expr.op_type == CondExpr:

            self.add_cond(op_expr.op.sdql_ir)
            self.cond_info['cond_if'] = op_expr.op.sdql_ir

            if self.is_joint:
                self.joint_info['joint_cond'] = op_expr.op

            self.cond_status = True
        if op_expr.op_type == AggrExpr:
            if op_expr.ret_type == OperationReturnType.DICT:
                self.agg_dict_info['aggr_dict'] = op_expr.op.aggr_op
                self.agg_dict_info['cond_if'] = self.cond_info['cond_if']
                self.agg_dict_info['cond_then'] = op_expr.op.aggr_op
                self.agg_dict_info['cond_else'] = op_expr.op.aggr_else
            else:
                self.cond_info['cond_then'] = op_expr.op.aggr_op
                self.cond_info['cond_else'] = op_expr.op.aggr_else
                self.sum_info['sum_op'] = self.cond_stmt

            self.last_func = LastIterFunc.Agg
        if op_expr.op_type == VirColEl:
            self.add_col_ins(col_name=op_expr.op.col_var,
                             col_expr=op_expr.op.col_expr)

            self.col_ins[op_expr.op.col_var] = op_expr.op.col_expr
        if op_expr.op_type == GroupByAgg:
            groupby_from = op_expr.op.groupby_from
            groupby_cols = op_expr.op.groupby_cols
            aggr_dict = op_expr.op.agg_dict

            self.groupby_aggr_info['groupby_cols'] = groupby_cols
            self.groupby_aggr_info['aggr_dict'] = aggr_dict

            self.set_groupby_aggr_key_part(groupby_from, groupby_cols)
            self.set_groupby_aggr_val_part(aggr_dict)

            self.set_groupby_aggr_aggr_op()

            if self.has_isin:
                ref_var = self.isin_op.get_ref_var()
                probe_field = self.isin_op.get_probe_field()
                self.cond_info['cond_then'] = IfExpr(condExpr=CompareExpr(compareType=CompareSymbol.NE,
                                                                          leftExpr=DicLookupExpr(ref_var,
                                                                                                 self.opt_on.key_access(
                                                                                                     probe_field)),
                                                                          rightExpr=ConstantExpr(None)),
                                                     thenBodyExpr=self.groupby_aggr_info['aggr_op'],
                                                     elseBodyExpr=EmptyDicConsExpr())
            else:
                self.cond_info['cond_then'] = self.groupby_aggr_info['aggr_op']
            self.cond_info['cond_else'] = EmptyDicConsExpr()

            self.set_groupby_aggr_let_val()

            vname_concat = f'x_{self.opt_on.name}_groupby_agg'
            var_concat = VarExpr(vname_concat)
            self.opt_on.add_context_variable(vname_concat, var_concat)
            sum_concat = SumExpr(varExpr=var_concat,
                                 dictExpr=self.groupby_aggr_info['aggr_var'],
                                 bodyExpr=DicConsExpr([(ConcatExpr(PairAccessExpr(var_concat, 0),
                                                                   PairAccessExpr(var_concat, 1)),
                                                        ConstantExpr(True))]),
                                 isAssignmentSum=True)

            vname_out = 'out'
            var_out = VarExpr(vname_out)
            self.opt_on.add_context_variable(vname_out, var_out)
            self.groupby_aggr_info['let_next'] = LetExpr(varExpr=var_out,
                                                         valExpr=sum_concat,
                                                         bodyExpr=ConstantExpr(True))

            self.last_func = LastIterFunc.GroupbyAgg

        if op_expr.op_type == ColProjExpr:
            for col in op_expr.op.proj_cols:
                self.add_col_proj((col,
                                   op_expr.op.proj_on.key_access(col)))

        if op_expr.op_type == MergeExpr:
            # detect(self) -> partition side
            if op_expr.op.left.name == self.opt_on.name:
                self.is_join_partition_side = True

                self.join_partition_info['partition_key'] = op_expr.op.left_on

                self.last_func = LastIterFunc.JoinPartition
            # detect(self) -> probe side
            elif op_expr.op.right.name == self.opt_on.name:
                self.is_join_probe_side = True

                self.join_probe_info['probe_key'] = op_expr.op.right_on

                self.last_func = LastIterFunc.JoinProbe
            else:
                if self.is_joint:
                    self.joint_info['partition_side'] = op_expr.op.left
                    self.joint_info['partition_key'] = op_expr.op.left_on
                    self.joint_info['probe_side'] = op_expr.op.right
                    self.joint_info['probe_key'] = op_expr.op.right_on
                    self.joint_info['how'] = op_expr.op.how

                    self.last_func = LastIterFunc.Joint
                else:
                    raise ValueError(str(op_expr))

        if op_expr.op_type == IsInExpr:
            self.isin_op = op_expr.op

            self.has_isin = True

        if op_expr.op_type == CalcExpr:
            self.last_func = LastIterFunc.Calculation

        if op_expr.op_type == ExternalExpr:
            self.cond_info['cond_if'] = op_expr.op

    @property
    def partition_frame(self):
        frame = JoinPartitionFrame(iter_on=self.opt_on,
                                   col_proj=self.col_proj)

        frame.add_key(self.join_partition_info['partition_key'])
        if type(self.cond_info['cond_if']) != ConstantExpr:
            frame.add_cond(self.cond_info['cond_if'])

        return frame

    @property
    def probe_frame(self):
        if not self.is_join_probe_side:
            raise ValueError()

        frame = JoinProbeFrame(self.opt_on)

        frame.add_key(self.join_probe_info['probe_key'])
        frame.add_cond(self.cond_info['cond_if'])
        frame.add_col_proj(self.col_proj)

        # print(self.join_probe_info['probe_key'])
        # print(self.cond_info['cond_if'])
        # print(self.col_proj)

        return frame

    @property
    def joint_frame(self):
        partition_frame = self.joint_info['partition_side'].get_partition_frame()
        probe_frame = self.joint_info['probe_side'].get_probe_frame()

        # define aggr_dict
        if self.last_func == LastIterFunc.GroupbyAgg:
            aggr_dict = self.groupby_aggr_info['aggr_dict']
        elif self.last_func == LastIterFunc.Agg:
            aggr_dict = self.agg_dict_info['aggr_dict']
        else:
            aggr_dict = None

        # define groupby_cols
        if self.groupby_aggr_info['groupby_cols']:
            groupby_cols = self.groupby_aggr_info['groupby_cols']
        else:
            groupby_cols = None

        # define joint_cond
        if self.joint_info['joint_cond']:
            joint_cond = self.joint_info['joint_cond']
        else:
            joint_cond = None

        tmp_joint_frame = JointFrame(partition=partition_frame,
                                     probe=probe_frame,
                                     joint=self.opt_on,
                                     col_ins=self.col_ins,
                                     col_proj=self.col_proj,
                                     groupby_cols=groupby_cols,
                                     aggr_dict=aggr_dict,
                                     joint_cond=joint_cond)

        if self.col_proj != tmp_joint_frame.col_proj:
            raise ValueError(f'Column Projection Not Applied to {self.opt_on.name}')

        return tmp_joint_frame

    def merge_partition_stmt(self, let_next=None) -> LetExpr:
        merge_left_on_ir = self.opt_on.key_access(self.last_merge_info['left_on'])

        if self.has_cond:
            part_left_op = IfExpr(condExpr=self.get_cond_ir(),
                                  thenBodyExpr=DicConsExpr([(
                                      merge_left_on_ir,
                                      self.get_col_proj_ir(MergeType.PARTITION)
                                  )]),
                                  elseBodyExpr=EmptyDicConsExpr())
        else:
            part_left_op = DicConsExpr([(
                merge_left_on_ir,
                self.get_col_proj_ir(MergeType.PARTITION)
            )])

        self.merge_left_info['merge_left_sum_op'] = part_left_op

        part_left_sum = SumExpr(varExpr=self.merge_left_info['merge_left_sum_el'],
                                dictExpr=self.merge_left_info['merge_left_sum_on'],
                                bodyExpr=self.merge_left_info['merge_left_sum_op'],
                                isAssignmentSum=True)

        self.merge_left_info['merge_left_let_val'] = part_left_sum
        self.merge_left_info['merge_left_let_next'] = ConstantExpr('placeholder_merge_partition_statement')

        if let_next:
            return LetExpr(varExpr=self.merge_left_info['merge_left_let_var'],
                           valExpr=self.merge_left_info['merge_left_let_val'],
                           bodyExpr=let_next)
        else:
            return LetExpr(varExpr=self.merge_left_info['merge_left_let_var'],
                           valExpr=self.merge_left_info['merge_left_let_val'],
                           bodyExpr=self.merge_left_info['merge_left_let_next'])

    def merge_probe_stmt(self, let_next=None, isAssign=False) -> LetExpr:
        merge_left_opt = self.last_merge_info['left'].get_opt(OptGoal.JoinPartition)
        merge_left_var = merge_left_opt.merge_left_info['merge_left_let_var']

        merge_right_on_ir = self.opt_on.key_access(self.last_merge_info['right_on'])

        if self.has_cond:
            if self.was_merge_probe and self.is_next_merge_partition:
                right_op = IfExpr(condExpr=self.get_cond_ir(),
                                  thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                           leftExpr=DicLookupExpr(
                                                                               dicExpr=merge_left_var,
                                                                               keyExpr=merge_right_on_ir),
                                                                           rightExpr=ConstantExpr(None)),
                                                      thenBodyExpr=DicConsExpr([(
                                                          self.opt_on.key_access(self.merge_left_info['merge_left_on']),
                                                          self.get_col_proj_ir(MergeType.PROBE)
                                                      )]),
                                                      elseBodyExpr=EmptyDicConsExpr()),
                                  elseBodyExpr=EmptyDicConsExpr())
            else:
                right_op = IfExpr(condExpr=self.get_cond_ir(),
                                  thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                           leftExpr=DicLookupExpr(
                                                                               dicExpr=merge_left_var,
                                                                               keyExpr=merge_right_on_ir),
                                                                           rightExpr=ConstantExpr(None)),
                                                      thenBodyExpr=DicConsExpr([(
                                                          merge_right_on_ir,
                                                          self.get_col_proj_ir(MergeType.PROBE)
                                                      )]),
                                                      elseBodyExpr=EmptyDicConsExpr()),
                                  elseBodyExpr=EmptyDicConsExpr())
        else:
            right_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                   leftExpr=DicLookupExpr(dicExpr=merge_left_var,
                                                                          keyExpr=merge_right_on_ir),
                                                   rightExpr=ConstantExpr(None)),
                              thenBodyExpr=DicConsExpr([(
                                  merge_right_on_ir,
                                  self.get_col_proj_ir(MergeType.PROBE)
                              )]),
                              elseBodyExpr=EmptyDicConsExpr())

        self.merge_right_info['merge_right_sum_op'] = right_op

        right_sum = SumExpr(varExpr=self.merge_right_info['merge_right_sum_el'],
                            dictExpr=self.merge_right_info['merge_right_sum_on'],
                            bodyExpr=self.merge_right_info['merge_right_sum_op'],
                            isAssignmentSum=isAssign)

        self.merge_right_info['merge_right_let_val'] = right_sum
        self.merge_right_info['merge_right_let_next'] = ConstantExpr('placeholder_merge_probe_statement')

        if let_next:
            let_expr = LetExpr(varExpr=self.merge_right_info['merge_right_let_var'],
                               valExpr=self.merge_right_info['merge_right_let_val'],
                               bodyExpr=let_next)
        else:
            let_expr = LetExpr(varExpr=self.merge_right_info['merge_right_let_var'],
                               valExpr=self.merge_right_info['merge_right_let_val'],
                               bodyExpr=self.merge_right_info['merge_right_let_next'])

        return merge_left_opt.merge_partition_stmt(let_expr)

    @property
    def was_merge_partition(self):
        for op_expr in self.opt_on.operations:
            if op_expr.op_type == MergeExpr:
                if op_expr.op.left.name == self.opt_on.name:
                    return True
        return False

    @property
    def was_merge_probe(self):
        for op_expr in self.opt_on.operations:
            if op_expr.op_type == MergeExpr:
                if op_expr.op.right.name == self.opt_on.name:
                    return True
        return False

    @property
    def is_next_merge_partition(self):
        if self.last_merge_info['left'].name == self.opt_on.name:
            return True
        else:
            return False

    @property
    def is_next_merge_probe(self):
        if self.last_merge_info['right'].name == self.opt_on.name:
            return True
        else:
            return False

    @property
    def groupby_aggr_with_merge_stmt(self):
        groupby_cols = self.groupby_aggr_info['groupby_cols']
        aggr_dict = self.groupby_aggr_info['aggr_dict']

        right_on_ir = self.opt_on.key_access(self.merge_right_info['merge_right_on'])

        # aggr_key_ir
        key_rec_list = []
        for i in groupby_cols:
            if i == self.merge_right_info['merge_right_on']:
                key_rec_list.append((i, self.opt_on.key_access(i)))
            if i in self.last_merge_info['left'].cols_out:
                key_rec_list.append(
                    (i, RecAccessExpr(recExpr=DicLookupExpr(dicExpr=self.last_merge_info['left'].var_probe,
                                                            keyExpr=self.last_merge_info['left'].key_access(i)),
                                      fieldName=i)))

        aggr_key_ir = RecConsExpr(key_rec_list)

        # aggr_val_ir
        val_rec_list = []
        if self.col_ins:
            for k in aggr_dict.keys():
                v = aggr_dict[k]
                if v.name in self.col_ins.keys():
                    col_expr = self.col_ins[v.name].sdql_ir
                else:
                    col_expr = v
                val_rec_list.append((k, col_expr))

        aggr_val_ir = RecConsExpr(val_rec_list)

        if self.has_cond:
            merge_groupby_aggr_op = IfExpr(condExpr=self.get_cond_ir(),
                                           thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                                    leftExpr=DicLookupExpr(
                                                                                        dicExpr=self.opt_on.var_expr,
                                                                                        keyExpr=right_on_ir),
                                                                                    rightExpr=ConstantExpr(None)),
                                                               thenBodyExpr=DicConsExpr([(
                                                                   aggr_key_ir,
                                                                   aggr_val_ir
                                                               )]),
                                                               elseBodyExpr=EmptyDicConsExpr()),
                                           elseBodyExpr=EmptyDicConsExpr())
        else:
            merge_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                leftExpr=DicLookupExpr(dicExpr=self.opt_on.var_expr,
                                                                                       keyExpr=right_on_ir),
                                                                rightExpr=ConstantExpr(None)),
                                           thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                      aggr_val_ir
                                                                      )]),
                                           elseBodyExpr=EmptyDicConsExpr())

        next_sum = SumExpr(varExpr=self.opt_on.iter_el.el,
                           dictExpr=self.opt_on.var_expr,
                           bodyExpr=merge_groupby_aggr_op,
                           isAssignmentSum=False)

        result = VarExpr('result')

        next_let = LetExpr(varExpr=self.opt_on.var_probe,
                           valExpr=next_sum,
                           bodyExpr=LetExpr(varExpr=result,
                                            valExpr=SumBuilder(
                                                lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]),
                                                self.opt_on.var_probe,
                                                True),
                                            bodyExpr=LetExpr(VarExpr('out'), result, ConstantExpr(True))))

        return self.last_merge_info['left'].merge_probe_stmt(let_next=next_let)

    @property
    def agg_dict_stmt(self):
        if len(self.agg_dict_info['cond_then'].keys()) == 1:
            agg_list = []
            for agg_key in self.agg_dict_info['cond_then'].keys():
                v = self.agg_dict_info['cond_then'][agg_key]
                if self.col_ins:
                    if v in self.col_ins.keys():
                        col_expr = self.col_ins[v].sdql_ir
                    else:
                        col_expr = v
                    agg_list.append((agg_key, col_expr))
                else:
                    agg_list.append((agg_key, v))

            sum_op = DicConsExpr(agg_list)

            if self.has_cond:
                sum_op = IfExpr(self.get_cond_ir(),
                                sum_op,
                                EmptyDicConsExpr())

            self.agg_dict_info['sum_op'] = sum_op

            out = VarExpr('out')
            self.opt_on.add_context_variable(vname='out',
                                             vobj=out)

            return LetExpr(out,
                           SumExpr(self.agg_dict_info['sum_el'],
                                   self.agg_dict_info['sum_on'],
                                   self.agg_dict_info['sum_op']),
                           ConstantExpr(True))
        else:
            raise NotImplementedError

    @property
    def info(self):
        if self.opt_goal == OptGoal.JoinPartition:
            col_proj_ir = self.get_col_proj_ir(MergeType.PARTITION)
        elif self.opt_goal == OptGoal.JoinProbe:
            col_proj_ir = self.get_col_proj_ir(MergeType.PROBE)
        else:
            col_proj_ir = self.get_col_proj_ir(MergeType.NONE)

        return {
            'cond': self.get_cond_ir(),
            'col_proj': col_proj_ir,
        }

    @property
    def output(self) -> LetExpr:
        if self.last_func == LastIterFunc.Agg:
            op_expr = self.opt_on.peak()
            if op_expr.ret_type == OperationReturnType.DICT:
                # Q19 -> this way, sir
                if self.is_joint:
                    return self.joint_frame.sdql_ir
                # Q6 -> this way, sir
                return self.agg_dict_stmt
            else:
                result = VarExpr('result')
                return LetExpr(result,
                               self.sum_stmt,
                               LetExpr(VarExpr('out'),
                                       result,
                                       ConstantExpr(True)))
        if self.last_func == LastIterFunc.GroupbyAgg:
            if self.is_joint:
                # Q3 -> this way, sir
                # Q16 -> this way, sir
                return self.joint_frame.sdql_ir
            if self.has_isin:
                # Q4 -> this way, sir
                return self.isin_op.get_ref_ir(self.groupby_aggr_stmt)
            if self.get_cond_after_groupby_agg():
                return self.get_groupby_agg_having_stmt()
            else:
                # Q1 -> this way, sir
                return self.groupby_aggr_stmt
        if self.last_func == LastIterFunc.Joint:
            return self.joint_frame.sdql_ir
        if self.last_func == LastIterFunc.Calculation:
            return self.joint_frame.sdql_ir
        # if self.last_func == LastIterFunc.JoinPartition:
        #     return self.merge_partition_stmt()
        # if self.last_func == LastIterFunc.JoinProbe:
        #     return self.merge_probe_stmt()
        else:
            raise ValueError()
