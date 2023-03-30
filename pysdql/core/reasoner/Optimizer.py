from pysdql.core.utils.flex_check import (
    is_cond,
    map_name_to_dataset,
)

from pysdql.core.enums.EnumUtil import (
    LastIterFunc,
    MergeType,
    OpRetType,
)

from pysdql.core.interfaces.availability.Replaceable import Replaceable

from pysdql.core.exprs.advanced.ColOpExprs import ColOpExternal
from pysdql.core.exprs.advanced.AggrOpExprs import (
    AggrBinOp,
    AggrOpFilter,
    AggrOpRename,
)
from pysdql.core.exprs.advanced.AggrFuncExprs import AggrNuniqueFunc
from pysdql.core.exprs.advanced.ColBridgeExprs import (
    ColElBridge,
    ColOpBridge,
)
from pysdql.core.exprs.advanced.BinCondExpr import BinCondExpr
from pysdql.core.exprs.advanced.FreeStateExprs import FreeStateVar
from pysdql.core.exprs.advanced.MergeExprs import (
    MergeExpr,
    MergeIndicator,
)
from pysdql.core.exprs.advanced.ColAlterExprs import (
    OldColRename,
    NewColInsert,
    NewColListInsert,
)
from pysdql.core.exprs.advanced.ColOpIsinExpr import ColOpIsin
from pysdql.core.exprs.advanced.ColProjExprs import (
    ColProj,
    ColProjUnique,
    ColProjRename,
)

from pysdql.core.exprs.carrier.PandasFuncExpr import DropDuplFunc
from pysdql.core.exprs.carrier.OpExpr import OpExpr

from pysdql.core.exprs.complex.AggrExpr import AggrExpr
from pysdql.core.exprs.complex.GroupbyAggrExpr import GroupbyAggrExpr
from pysdql.core.exprs.complex.IterForm import IterForm
from pysdql.core.exprs.complex.FlexChain import OpChain

from pysdql.core.reasoner.AggrFrame import AggrFrame
from pysdql.core.reasoner.GroupbyAggrFrame import GroupbyAggrFrame
from pysdql.core.reasoner.JointFrame import JointFrame
from pysdql.core.reasoner.JoinPartFrame import JoinPartFrame
from pysdql.core.reasoner.JoinProbeFrame import JoinProbeFrame

from pysdql.core.killer.Retriever import Retriever
from pysdql.core.killer.SDQLInspector import SDQLInspector

from pysdql.core.prototype.basic.sdql_ir import *
from pysdql.extlib.sdqlpy.sdql_lib import sr_dict

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

        self.vname_groupby_agg_concat = f'{opt_on.name}_groupby_agg_concat'
        self.var_groupby_agg_concat = VarExpr(self.vname_groupby_agg_concat)

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

            'merge_left_let_var': self.opt_on.var_part,
            'merge_left_let_val': ConstantExpr(None),
            'merge_left_let_next': ConstantExpr(None)
        }

        self.merge_right_info = {
            'merge_right_on': '',

            'merge_right_sum_el': opt_on.iter_el.sdql_ir,
            'merge_right_sum_on': opt_on.var_expr,
            'merge_right_sum_op': ConstantExpr(None),

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

        self.op_chain = OpChain(self.opt_on)

    @property
    def has_cond(self):
        for op_expr in self.opt_on.operations:
            if op_expr.op_type == BinCondExpr:
                return True
        return False

    def get_cond_after_groupby_agg(self):
        groupby_agg_located = False
        for op_expr in self.opt_on.operations:
            if op_expr.op_type == GroupbyAggrExpr:
                groupby_agg_located = True
            if op_expr.op_type == BinCondExpr:
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
        if op_expr.op_type == BinCondExpr:

            self.add_cond(op_expr.op.sdql_ir)
            self.cond_info['cond_if'] = op_expr.op.sdql_ir

            if self.is_joint:
                self.joint_info['joint_cond'] = op_expr.op

            self.cond_status = True
        if op_expr.op_type == AggrExpr:
            if op_expr.ret_type == OpRetType.DICT:
                self.agg_dict_info['aggr_dict'] = op_expr.op.aggr_op
                self.agg_dict_info['cond_if'] = self.cond_info['cond_if']
                self.agg_dict_info['cond_then'] = op_expr.op.aggr_op
                self.agg_dict_info['cond_else'] = op_expr.op.aggr_else
            else:
                self.cond_info['cond_then'] = op_expr.op.aggr_op
                self.cond_info['cond_else'] = op_expr.op.aggr_else
                self.sum_info['sum_op'] = self.cond_stmt
        if op_expr.op_type == NewColInsert:
            self.add_col_ins(col_name=op_expr.op.col_var,
                             col_expr=op_expr.op.col_expr)

            self.col_ins[op_expr.op.col_var] = op_expr.op.col_expr
        if op_expr.op_type == GroupbyAggrExpr:
            groupby_from = op_expr.op.groupby_from
            groupby_cols = op_expr.op.groupby_cols
            aggr_dict = op_expr.op.aggr_dict

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

        if op_expr.op_type == ColProj:
            for col in op_expr.op.proj_cols:
                self.add_col_proj((col,
                                   op_expr.op.proj_on.key_access(col)))

        if op_expr.op_type == MergeExpr:
            # detect(self) -> partition side
            if op_expr.op.left.name == self.opt_on.name:
                self.is_join_partition_side = True

                self.join_partition_info['partition_key'] = op_expr.op.left_on
            # detect(self) -> probe side
            elif op_expr.op.right.name == self.opt_on.name:
                self.is_join_probe_side = True

                self.join_probe_info['probe_key'] = op_expr.op.right_on
            else:
                if self.is_joint:
                    self.joint_info['partition_side'] = op_expr.op.left
                    self.joint_info['partition_key'] = op_expr.op.left_on
                    self.joint_info['probe_side'] = op_expr.op.right
                    self.joint_info['probe_key'] = op_expr.op.right_on
                    self.joint_info['how'] = op_expr.op.how
                else:
                    raise ValueError(str(op_expr))

        if op_expr.op_type == ColOpIsin:
            self.isin_op = op_expr.op

            self.has_isin = True

        if op_expr.op_type == ColOpExternal:
            self.cond_info['cond_if'] = op_expr.op

    @property
    def partition_frame(self):
        frame = JoinPartFrame(iter_on=self.opt_on,
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

        # print(self.opt_on.name)
        # print(self.col_proj)

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

    @property
    def aggr_frame(self):
        return AggrFrame(self.opt_on)

    @property
    def retriever(self) -> Retriever:
        return self.opt_on.get_retriever()

    @property
    def last_func(self):
        return self.retriever.find_last_iter(as_enum=True)

    @property
    def output(self) -> LetExpr:
        # self.op_chain.infer(entrance=True)

        if self.last_func == LastIterFunc.Agg:
            op_expr = self.retriever.find_aggr(body_only=False)
            if op_expr.op.aggr_on.name != self.opt_on.name:
                for k in op_expr.op.aggr_on.context_constant.keys():
                    self.opt_on.context_constant[k] = op_expr.op.aggr_on.context_constant[k]

                if op_expr.op.aggr_on.is_joint:
                    return op_expr.op.aggr_on.joint_frame.sdql_ir
                else:
                    return AggrFrame(op_expr.op.aggr_on).sdql_ir
                # return SDQLInspector.rename_last_binding(AggrFrame(op_expr.op.aggr_on).sdql_ir,
                #                                          self.opt_on.name)

            if op_expr.ret_type == OpRetType.DICT:
                # Q19
                if self.is_joint:
                    return self.joint_frame.sdql_ir
                # Q6
                # Q6_1
                return AggrFrame(self.opt_on).sdql_ir
            if op_expr.ret_type == OpRetType.FLOAT:
                # Q6_2
                return AggrFrame(self.opt_on).sdql_ir

            raise NotImplementedError
        elif self.last_func == LastIterFunc.GroupbyAgg:
            if self.is_joint:
                # Q3
                # Q16
                return self.joint_frame.sdql_ir
            # if self.get_cond_after_groupby_agg():
            #     return self.get_groupby_agg_having_stmt()

            # Q1
            # Q4
            # Q22
            return GroupbyAggrFrame(self.opt_on).sdql_ir
        elif self.last_func == LastIterFunc.Joint:
            # Q15
            # Q20
            return self.joint_frame.sdql_ir
        elif self.last_func == LastIterFunc.Calc:
            # Q14
            op_expr = self.retriever.find_last_iter(body_only=False)

            if op_expr.op.on.name != self.opt_on.name:
                for k in op_expr.op.on.context_constant.keys():
                    self.opt_on.context_constant[k] = op_expr.op.on.context_constant[k]

                # print(op_expr.op.on.operations)

                return op_expr.op.on.joint_frame.sdql_ir

            if self.is_joint:
                return self.joint_frame.sdql_ir
        else:
            last_op = self.retriever.find_last_iter()
            print('Unknown Last Operation:', type(last_op), last_op)
            raise NotImplementedError

    def get_unopt_context(self,
                          rename_last='',
                          conflict_rename_indicator=False,
                          process_until=None,
                          def_const=False,
                          ):
        this_name = self.opt_on.current_name

        rename_indicator = False

        # print(this_name)
        # print(self.opt_on.operations)

        allow_projection = False

        unopt_context = []
        unopt_count = 0

        tmp_vn_on = map_name_to_dataset(self.opt_on.name)
        tmp_el_on = 'x'
        tmp_vn_nx = f'{this_name}_{unopt_count}'

        col_attach_cache = {}
        col_attach_name = f'default_attach_columns_to'
        
        if def_const:
            for k in self.opt_on.context_constant.keys():
                unopt_context.append(LetExpr(varExpr=self.opt_on.get_const_var(k),
                                             valExpr=ConstantExpr(k),
                                             bodyExpr=ConstantExpr(True)))

        for op_expr in self.opt_on.operations:
            if unopt_count != 0:
                tmp_vn_on = f'{this_name}_{unopt_count - 1}'
                tmp_vn_nx = f'{this_name}_{unopt_count}'

            op_body = op_expr.op

            if process_until:
                if self.retriever.equals(process_until, op_body):
                    break

            if isinstance(op_body, ColElBridge):
                if not col_attach_cache:
                    col_attach_name = f'{op_body.create_from.current_name}_attach_to_{op_body.attach_to.current_name}'

                    unopt_context += op_body.create_from.get_context_unopt(rename_last=col_attach_name,
                                                                           process_until=op_body,
                                                                           def_const=True,
                                                                           )

                    col_attach_cache[op_body.col_to.field] = op_body
                else:
                    col_attach_cache[op_body.col_to.field] = op_body

                continue
            elif isinstance(op_body, ColOpBridge):
                if not col_attach_cache:
                    col_attach_name = f'{op_body.create_from.current_name}_attach_to_{op_body.attach_to.current_name}'

                    op_body.create_from.push(OpExpr(op_obj=NewColInsert(op_body.col_to.field,
                                                                        op_body.col_from.sdql_ir),
                                                    op_on=op_body.create_from,
                                                    op_iter=False))

                    unopt_context += op_body.create_from.get_context_unopt(rename_last=col_attach_name,
                                                                           process_until=op_body,
                                                                           def_const=True,
                                                                           )

                    col_attach_cache[op_body.col_to.field] = op_body
                else:
                    col_attach_cache[op_body.col_to.field] = op_body

                continue
            elif isinstance(op_body, FreeStateVar):
                # print(op_body)
                continue
            elif isinstance(op_body, DropDuplFunc):
                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                rec_list = [(i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)) for i in op_body.unique_cols]

                tmp_it.iter_op = DicConsExpr([(RecConsExpr(rec_list), ConstantExpr(True))])

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, ColProjUnique):
                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                final_cols = []

                for i in op_body.proj_cols:
                    final_cols.append(i)

                rec_list = [(i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)) for i in final_cols]

                proj_op = DicConsExpr([(RecConsExpr(rec_list), ConstantExpr(True))])

                tmp_it.iter_op = proj_op

                # tmp_it.iter_op = sr_dict(dict(proj_op.initialPairs))

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, ColProj):
                if col_attach_cache:
                    tmp_it = IterForm(col_attach_name, tmp_el_on)

                    rec_list = [(i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)) for i in col_attach_cache.keys()]

                    proj_op = DicConsExpr([(RecConsExpr(rec_list), ConstantExpr(True))])

                    tmp_it.iter_op = proj_op

                    # tmp_it.iter_op = sr_dict(dict(proj_op.initialPairs))

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=tmp_it.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )
                elif self.retriever.check_last(op_body) \
                        or (len(op_body.proj_cols) == 1
                            and self.retriever.check_as_merge_key(op_body.proj_cols[0])) \
                        or (False) \
                        or (allow_projection):

                    # self.retriever.check_next(op_body, MergeExpr)

                    if self.retriever.findall_col_insert_as_list():
                        continue

                    tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                    final_cols = []

                    for i in op_body.proj_cols:
                        final_cols.append(i)

                    for j in self.retriever.findall_additional_columns():
                        if j not in op_body.proj_cols:
                            final_cols.append(j)

                    rec_list = [(i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)) for i in final_cols]

                    proj_op = DicConsExpr([(RecConsExpr(rec_list), ConstantExpr(True))])
                    # proj_op = sr_dict(dict(proj_op.initialPairs))

                    # tmp_it.iter_op = proj_op

                    tmp_it.iter_op = DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                   proj_op)])

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=tmp_it.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )

                    unopt_count += 1

                    tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                    tmp_vn_nx = f'{this_name}_{unopt_count}'

                    tmp_it_2 = IterForm(tmp_vn_on_2, tmp_el_on)

                    tmp_it_2.iter_op = PairAccessExpr(VarExpr(tmp_el_on), 1)

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=tmp_it_2.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )
                else:
                    # if allow_projection:
                    #     if self.retriever.findall_col_insert_as_list():
                    #         continue
                    #
                    #     tmp_it = IterForm(tmp_vn_on, tmp_el_on)
                    #
                    #     final_cols = []
                    #
                    #     for i in op_body.proj_cols:
                    #         final_cols.append(i)
                    #
                    #     for j in self.retriever.findall_additional_columns():
                    #         final_cols.append(j)
                    #
                    #     rec_list = [(i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)) for i in final_cols]
                    #
                    #     proj_op = DicConsExpr([(RecConsExpr(rec_list), ConstantExpr(True))])
                    #
                    #     tmp_it.iter_op = proj_op
                    #
                    #     # tmp_it.iter_op = sr_dict(dict(proj_op.initialPairs))
                    #
                    #     unopt_context.append(
                    #         LetExpr(varExpr=VarExpr(tmp_vn_nx),
                    #                 valExpr=tmp_it.sdql_ir,
                    #                 bodyExpr=ConstantExpr(True))
                    #     )
                    # else:
                    continue
            elif isinstance(op_body, ColProjRename):
                if conflict_rename_indicator:
                    tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                    if op_body.is_left:
                        # print(op_body.from_left)
                        rec_list = []

                        used = []

                        for i, j in zip(op_body.from_left, op_body.to_left):
                            if j == i:
                                rec_list.append((j, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                            else:
                                if j not in used:
                                    rec_list.append((j, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                                    rec_list.append((i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                                    used.append(j)

                        # print(f'rename for {self.opt_on.name}, to {op_body.to_left}')
                    elif op_body.is_right:
                        # print(op_body.from_right)
                        rec_list = []

                        used = []

                        for i, j in zip(op_body.from_right, op_body.to_right):
                            if j == i:
                                rec_list.append((j, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                            else:
                                if j not in used:
                                    rec_list.append((j, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                                    rec_list.append((i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                                    used.append(j)

                        # print(f'rename for {self.opt_on.name}, to {op_body.to_right}')
                    else:
                        # print(f'Unexpected column rename and projection: {op_body}')
                        continue

                    proj_op = DicConsExpr([(RecConsExpr(rec_list), ConstantExpr(True))])

                    tmp_it.iter_op = proj_op

                    tmp_it.iter_op = DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                   sr_dict(dict(proj_op.initialPairs)))])

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=tmp_it.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )

                    unopt_count += 1

                    tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                    tmp_vn_nx = f'{this_name}_{unopt_count}'

                    tmp_it_2 = IterForm(tmp_vn_on_2, tmp_el_on)

                    tmp_it_2.iter_op = PairAccessExpr(VarExpr(tmp_el_on), 1)

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=tmp_it_2.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )
                else:
                    continue
            elif is_cond(op_body):
                if isinstance(op_body, (BinCondExpr, ColOpExternal)):
                    if op_body.is_apply_cond:
                        continue

                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                free_vars = self.retriever.find_free_vars(op_body)

                if free_vars:
                    for free_vname in free_vars.keys():
                        free_expr = free_vars[free_vname]

                        unopt_context += free_expr.create_from.get_context_unopt(
                            def_const=True,
                        )

                # if isinstance(op_body, CondExpr):
                #     print(self.retriever.find_calc_in_cond(op_body))

                # if any([(i not in self.opt_on.cols_out)
                #         & (i not in self.retriever.find_cols_used('groupby_aggr'))
                #         & (i not in self.retriever.findall_col_insert().keys())
                #         for i in self.retriever.find_cols(op_body)]):
                #
                #     col_mapper = {}
                #     col_relations = {}
                #
                #     prev_agg_name_for_all = ""
                #
                #     for i in self.retriever.find_cols(op_body, as_expr=True):
                #         if i.field not in self.opt_on.columns:
                #             if i.relation.name not in col_relations.keys():
                #                 this_prev_agg_name = f'{i.relation.current_name}_{i.relation.unopt_count + 1}'
                #                 col_mapper[i.field] = VarExpr(this_prev_agg_name)
                #
                #                 print(f'we are here {i.relation}')
                #
                #                 col_relations[i.relation.name] = i.relation.get_opt().get_unopt_sdqlir()
                #                 prev_agg_name_for_all = this_prev_agg_name
                #             else:
                #                 col_mapper[i.field] = VarExpr(prev_agg_name_for_all)
                #         else:
                #             col_mapper[i.field] = PairAccessExpr(VarExpr(tmp_el_on), 0)
                #
                #     # print(col_mapper)
                #
                #     for k in col_relations.keys():
                #         unopt_context.append(
                #             SDQLInspector.rename_last_binding(col_relations[k],
                #                                               prev_agg_name_for_all,
                #                                               with_res=False,
                #                                               keep_the=-3)
                #         )
                #
                #     if isinstance(op_body, CondExpr):
                #         tmp_it.iter_cond.append(op_body.replace(rec=None, inplace=False, mapper=col_mapper))
                #     if isinstance(op_body, ColExtExpr):
                #         tmp_it.iter_cond.append(op_body.replace(rec=None, inplace=False, mapper=col_mapper).sdql_ir)
                # else:
                #     tmp_it.iter_cond.append(op_body)

                tmp_it.iter_cond.append(op_body)

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, AggrOpFilter):
                tmp_pairs = op_body.get_in_pairs()

                tmp_calc_value = f'tmp_var_{SDQLInspector.find_a_descriptor(tmp_pairs[1].sdql_ir)}'

                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                tmp_it.iter_op = SDQLInspector.replace_access(tmp_pairs[1].sdql_ir,
                                                                     PairAccessExpr(VarExpr(tmp_el_on), 0))

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_calc_value),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

                tmp_it_2 = IterForm(tmp_vn_on, tmp_el_on)

                # tmp_aggr_value = SDQLInspector.replace_access(tmp_pairs[2].sdql_ir,
                #                                               PairAccessExpr(VarExpr(tmp_el_on), 0))
                #
                # tmp_it_2.iter_cond.append(CompareExpr(tmp_pairs[0],
                #                                       VarExpr(tmp_calc_value),
                #                                       tmp_aggr_value))

                tmp_aggr_value = SDQLInspector.replace_access(tmp_pairs[2].sdql_ir,
                                                              PairAccessExpr(VarExpr(tmp_el_on), 0))

                groupby_rec = RecConsExpr([(i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i))
                                      for i in op_body.groupby_cols])

                # tmp_2_body = sr_dict({groupby_rec: tmp_aggr_value})
                # tmp_it_2.iter_op = DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0), tmp_2_body)])

                tmp_2_body = DicConsExpr([(groupby_rec, tmp_aggr_value)])

                tmp_it_2.iter_op = tmp_2_body

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it_2.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

                unopt_count += 1

                tmp_vn_on_3 = f'{this_name}_{unopt_count - 1}'
                tmp_vn_nx_3 = f'{this_name}_{unopt_count}'

                tmp_it_3 = IterForm(tmp_vn_on_3, tmp_el_on)

                # DicLookupExpr(VarExpr(tmp_vn_nx),
                #               groupby_rec)
                #

                tmp_it_3.iter_cond.append(CompareExpr(tmp_pairs[0],
                                                      VarExpr(tmp_calc_value),
                                                      PairAccessExpr(VarExpr(tmp_el_on), 1)))

                tmp_it_3.iter_op = DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0), ConstantExpr(True))])

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx_3),
                            valExpr=tmp_it_3.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

                unopt_count += 1

                tmp_vn_on_4 = f'{this_name}_{unopt_count - 1}'
                tmp_vn_nx_4 = f'{this_name}_{unopt_count}'

                tmp_it_4 = IterForm(tmp_vn_on, tmp_el_on)

                tmp_it_4.iter_cond.append(CompareExpr(CompareSymbol.NE,
                                                      DicLookupExpr(VarExpr(tmp_vn_nx_3),
                                                                    groupby_rec),
                                                      ConstantExpr(None)))

                tmp_it_4.iter_op = DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0), ConstantExpr(True))])

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx_4),
                            valExpr=tmp_it_4.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, (OldColRename, NewColInsert)):
                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                tmp_it.iter_op = op_body

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, ColOpIsin):
                # print(f'current dataframe: {this_name}')
                #
                # print(self.opt_on.operations)
                #
                # raise NotImplementedError

                if op_body.probe_on.current_name != this_name:
                    continue

                prev_isin_count = 0
                prev_ops_name = f'{op_body.part_on.current_name}_{op_body.probe_on.current_name}_isin_pre_ops'

                for o in op_body.part_on.get_context_unopt(rename_last=prev_ops_name,
                                                           process_until=op_body,
                                                           def_const=True,):
                    unopt_context.append(o)
                    prev_isin_count += 1

                prev_ops_name = map_name_to_dataset(op_body.part_on.name) if prev_isin_count == 0 else prev_ops_name

                part_name = map_name_to_dataset(op_body.part_on.name)
                probe_name = map_name_to_dataset(op_body.probe_on.name)

                last_build_on = prev_ops_name
                isin_build_name = f'{op_body.part_on.current_name}_{op_body.probe_on.current_name}_isin_build_index'

                if op_body.part_on.unopt_count == 0:
                    tmp_it_1 = IterForm(prev_ops_name, tmp_el_on)

                    tmp_it_1.iter_op = DicConsExpr(
                        [(RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), op_body.col_part.field),
                          ConstantExpr(True))])

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(isin_build_name),
                                valExpr=tmp_it_1.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )
                else:
                    tmp_it_1 = IterForm(last_build_on, tmp_el_on)

                    tmp_it_1.iter_op = DicConsExpr(
                        [(RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), op_body.col_part.field),
                          ConstantExpr(True))])

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(isin_build_name),
                                valExpr=tmp_it_1.sdql_ir,
                                bodyExpr=ConstantExpr(True))
                    )

                if unopt_count == 0:
                    tmp_it_2 = IterForm(probe_name, tmp_el_on)
                else:
                    tmp_it_2 = IterForm(tmp_vn_on, tmp_el_on)

                cond_symbol = CompareSymbol.EQ if op_body.isinvert else CompareSymbol.NE

                tmp_it_2.iter_op = IfExpr(CompareExpr(cond_symbol,
                                                      DicLookupExpr(VarExpr(isin_build_name),
                                                                    RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on),
                                                                                                 0),
                                                                                  op_body.col_probe.field)
                                                                    ),
                                                      ConstantExpr(None)),
                                          DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                        PairAccessExpr(VarExpr(tmp_el_on), 1))]),
                                          ConstantExpr(None))

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it_2.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, MergeExpr):
                overlap_cols = list(set(op_body.left.cols_out).intersection(op_body.right.cols_out))

                if overlap_cols:
                    all_cols_used = self.retriever.findall_cols_used(as_owner=False,
                                                                     only_next=False)

                    if any([(f'{i}_x' in all_cols_used) | (f'{i}_y' in all_cols_used) for i in overlap_cols]):
                        # print(f'{{\n'
                        #       f'    rename {overlap_cols}\n'
                        #       f'    {op_body.how} join\n'
                        #       f'    for {this_name}\n'
                        #       f'}}')

                        rename_indicator = True

                if op_body.how == 'inner':
                    if self.opt_on.current_name == op_body.joint.current_name:
                        # print(f'processing {self.opt_on.current_name} {op_body}')

                        build_prev_count = 0
                        build_prev_ops_name = f'{op_body.left.current_name}_{op_body.right.current_name}_build_pre_ops'

                        left_unopt_context = op_body.left.get_context_unopt(rename_last=build_prev_ops_name,
                                                                            conflict_rename_indicator=rename_indicator,
                                                                            process_until=op_body,
                                                                            def_const=True,)

                        for o in left_unopt_context:
                            unopt_context.append(o)
                            build_prev_count += 1

                        # if rename_indicator:
                        #     print(op_body.left.operations)
                        #     raise NotImplementedError

                        build_prev_ops_name = map_name_to_dataset(op_body.left.name) if build_prev_count == 0 else build_prev_ops_name

                        build_side_name = build_prev_ops_name

                        probe_prev_count = 0
                        probe_prev_ops_name = f'{op_body.left.current_name}_{op_body.right.current_name}_probe_pre_ops'

                        right_unopt_context = op_body.right.get_context_unopt(rename_last=probe_prev_ops_name,
                                                                              conflict_rename_indicator=rename_indicator,
                                                                              process_until=op_body,
                                                                              def_const=True,)

                        for o in right_unopt_context:
                            unopt_context.append(o)
                            probe_prev_count += 1

                        probe_prev_ops_name = map_name_to_dataset(op_body.right.name) if probe_prev_count == 0 else probe_prev_ops_name

                        probe_side_name = probe_prev_ops_name

                        tmp_it = IterForm(build_side_name, tmp_el_on)

                        tmp_build_side_name = f'{self.opt_on.name}_build_nest_dict'

                        if isinstance(op_body.left_on, str):
                            tmp_it.iter_op = DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                         str(op_body.left_on)),
                                                           sr_dict({PairAccessExpr(VarExpr(tmp_el_on), 0):
                                                                        PairAccessExpr(VarExpr(tmp_el_on), 1)}))])
                        elif isinstance(op_body.left_on, list):
                            tmp_it.iter_op = DicConsExpr([(RecConsExpr([(c,
                                                                         RecAccessExpr(
                                                                             PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                             c))
                                                                        for c in op_body.left_on]),
                                                           sr_dict({PairAccessExpr(VarExpr(tmp_el_on), 0):
                                                                        PairAccessExpr(VarExpr(tmp_el_on), 1)}))])

                        unopt_context.append(
                            LetExpr(varExpr=VarExpr(tmp_build_side_name),
                                    valExpr=tmp_it.sdql_ir,
                                    bodyExpr=ConstantExpr(True))
                        )

                        tmp_it = IterForm(probe_side_name, tmp_el_on)

                        if isinstance(op_body.right_on, str):
                            tmp_it.iter_cond.append(CompareExpr(CompareSymbol.NE,
                                                                DicLookupExpr(VarExpr(tmp_build_side_name),
                                                                              RecAccessExpr(
                                                                                  PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                                  op_body.right_on)),
                                                                ConstantExpr(None)))

                            inner_sum = SumExpr(VarExpr('y'),
                                                DicLookupExpr(VarExpr(tmp_build_side_name),
                                                              RecAccessExpr(
                                                                  PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                  op_body.right_on)
                                                              ),
                                                DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                         PairAccessExpr(VarExpr('y'), 0)),
                                                              ConstantExpr(True))])
                                                )

                            tmp_it.iter_op = inner_sum
                        elif isinstance(op_body.right_on, list):
                            target_rec_list = [(op_body.left_on[i],
                                                RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                              op_body.right_on[i]))
                                               for i in range(len(op_body.right_on))]

                            tmp_it.iter_cond.append(CompareExpr(CompareSymbol.NE,
                                                                DicLookupExpr(VarExpr(tmp_build_side_name),
                                                                              RecConsExpr(target_rec_list)),
                                                                ConstantExpr(None)))

                            inner_sum = SumExpr(VarExpr('y'),
                                                DicLookupExpr(VarExpr(tmp_build_side_name),
                                                              RecConsExpr(target_rec_list)),
                                                DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                         PairAccessExpr(VarExpr('y'), 0)),
                                                              ConstantExpr(True))])
                                                )

                            tmp_it.iter_op = inner_sum

                        unopt_context.append(
                            LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                    valExpr=tmp_it.sdql_ir,
                                    bodyExpr=ConstantExpr(True))
                        )
                    else:
                        continue
                elif op_body.how == 'right':
                    '''
                    v0 = orders_customer_probe.sum(lambda x: {x[0]: True} if (build_side[x[0].c_custkey] == None) else build_side[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True}))
                    '''
                    if self.opt_on.name == op_body.joint.name:
                        build_prev_count = 0
                        build_prev_ops_name = f'{op_body.left.current_name}_{op_body.right.current_name}_build_pre_ops'

                        for o in op_body.left.get_context_unopt(rename_last=build_prev_ops_name,
                                                                process_until=op_body,
                                                                def_const=True,):
                            unopt_context.append(o)
                            build_prev_count += 1

                        build_prev_ops_name = map_name_to_dataset(
                            op_body.left.name) if build_prev_count == 0 else build_prev_ops_name

                        build_side_name = build_prev_ops_name

                        probe_prev_count = 0
                        probe_prev_ops_name = f'{op_body.left.current_name}_{op_body.right.current_name}_probe_pre_ops'

                        for o in op_body.right.get_context_unopt(rename_last=probe_prev_ops_name,
                                                                 process_until=op_body,
                                                                 def_const=True,):
                            unopt_context.append(o)
                            probe_prev_count += 1

                        probe_prev_ops_name = map_name_to_dataset(
                            op_body.right.name) if probe_prev_count == 0 else probe_prev_ops_name

                        probe_side_name = probe_prev_ops_name

                        tmp_it = IterForm(build_side_name, tmp_el_on)

                        if isinstance(op_body.left_on, str):
                            tmp_it.iter_op = DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                         str(op_body.left_on)),
                                                           sr_dict({PairAccessExpr(VarExpr(tmp_el_on), 0):
                                                                        PairAccessExpr(VarExpr(tmp_el_on), 1)}))])
                        elif isinstance(op_body.left_on, list):
                            tmp_it.iter_op = DicConsExpr([(RecConsExpr([(c,
                                                                         RecAccessExpr(
                                                                             PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                             c))
                                                                        for c in op_body.left_on]),
                                                           sr_dict({PairAccessExpr(VarExpr(tmp_el_on), 0):
                                                                        PairAccessExpr(VarExpr(tmp_el_on), 1)}))])

                        tmp_build_side_name = f'{self.opt_on.name}_build_nest_dict'

                        unopt_context.append(
                            LetExpr(varExpr=VarExpr(tmp_build_side_name),
                                    valExpr=tmp_it.sdql_ir,
                                    bodyExpr=ConstantExpr(True))
                        )

                        tmp_it = IterForm(probe_side_name, tmp_el_on)

                        if isinstance(op_body.right_on, str):
                            cond_expr = CompareExpr(CompareSymbol.EQ,
                                                    DicLookupExpr(VarExpr(tmp_build_side_name),
                                                                  RecAccessExpr(
                                                                      PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                      op_body.right_on)),
                                                    ConstantExpr(None))

                            nested_sum = SumExpr(VarExpr('y'), DicLookupExpr(VarExpr(tmp_build_side_name),
                                                                             RecAccessExpr(
                                                                                 PairAccessExpr(
                                                                                     VarExpr(
                                                                                         tmp_el_on),
                                                                                     0),
                                                                                 op_body.right_on)),
                                                 DicConsExpr([(ConcatExpr(
                                                     PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                     PairAccessExpr(VarExpr('y'), 0)),
                                                               ConstantExpr(True))])
                                                 )

                            tmp_it.iter_cond.append(cond_expr)

                            tmp_it.iter_op = DicConsExpr([(PairAccessExpr(VarExpr(tmp_el_on), 0), ConstantExpr(True))])

                            tmp_it.iter_else = nested_sum

                        elif isinstance(op_body.right_on, list):
                            raise NotImplementedError
                            # tmp_it.iter_cond.append(CompareExpr(CompareSymbol.NE,
                            #                                     DicLookupExpr(VarExpr(tmp_build_side_name),
                            #                                                   RecConsExpr([(c,
                            #                                                                 RecAccessExpr(
                            #                                                                     PairAccessExpr(
                            #                                                                         VarExpr(tmp_el_on),
                            #                                                                         0),
                            #                                                                     c))
                            #                                                                for c in op_body.right_on])),
                            #                                     ConstantExpr(None)))
                            #
                            # tmp_it.iter_op = DicConsExpr([(SumExpr(VarExpr('y'), DicLookupExpr(VarExpr(tmp_build_side_name),
                            #                                                                    RecConsExpr([(c,
                            #                                                                                  RecAccessExpr(
                            #                                                                                      PairAccessExpr(
                            #                                                                                          VarExpr(
                            #                                                                                              tmp_el_on),
                            #                                                                                          0),
                            #                                                                                      c))
                            #                                                                                 for c in
                            #                                                                                 op_body.right_on])),
                            #                                        ConcatExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                            #                                                   PairAccessExpr(VarExpr('y'), 0))
                            #                                        ),
                            #                                ConstantExpr(True))])

                        unopt_context.append(
                            LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                    valExpr=tmp_it.sdql_ir,
                                    bodyExpr=ConstantExpr(True))
                        )
                    else:
                        continue
                elif op_body.how == 'outer':
                    if self.opt_on.name == op_body.joint.name:
                        merge_indicator = self.retriever.find_merge_indicator()

                        if merge_indicator:
                            if merge_indicator.left_only:
                                build_on = op_body.right
                                probe_on = op_body.left
                                build_key = op_body.right_on
                                probe_key = op_body.left_on

                                build_prev_count = 0
                                build_prev_ops_name = f'{build_on.current_name}_{probe_on.current_name}_build_pre_ops'

                                for o in build_on.get_context_unopt(rename_last=build_prev_ops_name,
                                                                    conflict_rename_indicator=rename_indicator,
                                                                    process_until=op_body,
                                                                    def_const=True,):
                                    unopt_context.append(o)
                                    build_prev_count += 1

                                build_prev_ops_name = map_name_to_dataset(
                                    build_on.name) if build_prev_count == 0 else build_prev_ops_name

                                build_side_name = build_prev_ops_name

                                probe_prev_count = 0
                                probe_prev_ops_name = f'{build_on.current_name}_{probe_on.current_name}_probe_pre_ops'

                                for o in probe_on.get_context_unopt(rename_last=probe_prev_ops_name,
                                                                    conflict_rename_indicator=rename_indicator,
                                                                    process_until=op_body,
                                                                    def_const=True,):
                                    unopt_context.append(o)
                                    probe_prev_count += 1

                                probe_prev_ops_name = map_name_to_dataset(
                                    probe_on.name) if probe_prev_count == 0 else probe_prev_ops_name

                                probe_side_name = probe_prev_ops_name

                                tmp_it = IterForm(build_side_name, tmp_el_on)

                                if isinstance(build_key, str):
                                    tmp_it.iter_op = DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                                 str(build_key)),
                                                                   sr_dict({PairAccessExpr(VarExpr(tmp_el_on), 0):
                                                                                PairAccessExpr(VarExpr(tmp_el_on),
                                                                                               1)}))])
                                elif isinstance(build_key, list):
                                    tmp_it.iter_op = DicConsExpr([(RecConsExpr([(c,
                                                                                 RecAccessExpr(
                                                                                     PairAccessExpr(VarExpr(tmp_el_on),
                                                                                                    0),
                                                                                     c))
                                                                                for c in build_key]),
                                                                   sr_dict({PairAccessExpr(VarExpr(tmp_el_on), 0):
                                                                                PairAccessExpr(VarExpr(tmp_el_on),
                                                                                               1)}))])

                                tmp_build_side_name = f'{self.opt_on.name}_build_nest_dict'

                                unopt_context.append(
                                    LetExpr(varExpr=VarExpr(tmp_build_side_name),
                                            valExpr=tmp_it.sdql_ir,
                                            bodyExpr=ConstantExpr(True))
                                )

                                tmp_it = IterForm(probe_side_name, tmp_el_on)

                                if isinstance(probe_key, str):
                                    cond_expr = CompareExpr(CompareSymbol.EQ,
                                                            DicLookupExpr(VarExpr(tmp_build_side_name),
                                                                          RecAccessExpr(
                                                                              PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                              probe_key)),
                                                            ConstantExpr(None))

                                    tmp_it.iter_cond.append(cond_expr)

                                    tmp_it.iter_op = DicConsExpr(
                                        [(PairAccessExpr(VarExpr(tmp_el_on), 0), ConstantExpr(True))])

                                    tmp_it.iter_else = ConstantExpr(None)

                                elif isinstance(op_body.right_on, list):
                                    raise NotImplementedError
                                    # tmp_it.iter_cond.append(CompareExpr(CompareSymbol.NE,
                                    #                                     DicLookupExpr(VarExpr(tmp_build_side_name),
                                    #                                                   RecConsExpr([(c,
                                    #                                                                 RecAccessExpr(
                                    #                                                                     PairAccessExpr(
                                    #                                                                         VarExpr(tmp_el_on),
                                    #                                                                         0),
                                    #                                                                     c))
                                    #                                                                for c in op_body.right_on])),
                                    #                                     ConstantExpr(None)))
                                    #
                                    # tmp_it.iter_op = DicConsExpr([(SumExpr(VarExpr('y'), DicLookupExpr(VarExpr(tmp_build_side_name),
                                    #                                                                    RecConsExpr([(c,
                                    #                                                                                  RecAccessExpr(
                                    #                                                                                      PairAccessExpr(
                                    #                                                                                          VarExpr(
                                    #                                                                                              tmp_el_on),
                                    #                                                                                          0),
                                    #                                                                                      c))
                                    #                                                                                 for c in
                                    #                                                                                 op_body.right_on])),
                                    #                                        ConcatExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                    #                                                   PairAccessExpr(VarExpr('y'), 0))
                                    #                                        ),
                                    #                                ConstantExpr(True))])

                                unopt_context.append(
                                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                            valExpr=tmp_it.sdql_ir,
                                            bodyExpr=ConstantExpr(True))
                                )
                            elif merge_indicator.right_only:
                                raise NotImplementedError
                            else:
                                raise NotImplementedError
                else:
                    print(f'Warning: Not implemented {op_body.how} join')

                    continue
            elif isinstance(op_body, NewColListInsert):
                if len(op_body.col_list) == 1:
                    target_expr = op_body.col_list[0]

                    if isinstance(target_expr, AggrBinOp):

                        # tmp_it_1 = IterForm(tmp_vn_on, tmp_el_on)

                        target_expr.on.push(OpExpr(op_obj=target_expr,
                                                   op_on=target_expr.on,
                                                   op_iter=True))

                        free_vname = target_expr.descriptor

                        unopt_context += target_expr.on.get_context_unopt(
                            rename_last=f'{op_body.col_var}_el_0_{free_vname}',
                            process_until=op_body,
                            def_const=True,
                        )

                        # unopt_context.append(
                        #     LetExpr(varExpr=VarExpr(tmp_vn_nx),
                        #             valExpr=tmp_it_1.sdql_ir,
                        #             bodyExpr=ConstantExpr(True))
                        # )

                        continue
                    elif isinstance(target_expr, AggrExpr):
                        free_vname = target_expr.descriptor

                        unopt_context += target_expr.aggr_on.get_context_unopt(
                            rename_last=f'{op_body.col_var}_el_0_{free_vname}',
                            process_until=op_body,
                            def_const=True,
                        )

                        # unopt_count += 1
                        #
                        # tmp_vn_nx = f'{this_name}_{unopt_count}'
                        #
                        # unopt_context.append(
                        #     LetExpr(varExpr=VarExpr(tmp_vn_nx),
                        #             valExpr=DicConsExpr([(
                        #                 RecConsExpr([(
                        #                    op_body.col_var,
                        #                    RecAccessExpr(VarExpr(f'{op_body.col_var}_el_0_{free_vname}'), free_vname)
                        #                 )]),
                        #                 ConstantExpr(True))]),
                        #             bodyExpr=ConstantExpr(True))
                        # )
                    else:
                        raise NotImplementedError(f'{type(op_body)} -> {op_body}')

            elif isinstance(op_body, AggrExpr):
                aggr_rename = self.retriever.find_aggr_rename()

                if len(list(op_body.aggr_op.keys())) == 1:
                    if list(op_body.aggr_op.keys())[0] in self.retriever.findall_cols_for_calc():
                        continue

                    if list(op_body.aggr_op.keys())[0] == 'sum_agg':
                        # op_body.is_multi_col_op
                        if isinstance(self.retriever.find_last_iter(), AggrExpr):
                            tmp_it_1 = IterForm(tmp_vn_on, tmp_el_on)

                            tmp_it_1.iter_op = SDQLInspector.replace_access(op_body.aggr_op['sum_agg'],
                                                                            PairAccessExpr(VarExpr(tmp_el_on), 0))

                            if aggr_rename:
                                unopt_context.append(
                                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                            valExpr=tmp_it_1.sdql_ir,
                                            bodyExpr=ConstantExpr(True))
                                )

                                unopt_count += 1

                                tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                                tmp_vn_nx = f'{this_name}_{unopt_count}'

                                unopt_context.append(
                                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                            valExpr=DicConsExpr([(
                                                RecConsExpr([(
                                                    aggr_rename.rename_to,
                                                    VarExpr(tmp_vn_on_2)
                                                )]),
                                                ConstantExpr(True))]),
                                            bodyExpr=ConstantExpr(True))
                                )
                            else:
                                unopt_context.append(
                                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                            valExpr=tmp_it_1.sdql_ir,
                                            bodyExpr=ConstantExpr(True))
                                )

                        continue

                    if op_body.is_single_col_op:
                        the_tuple = list(op_body.origin_dict.items())[0]

                        aggr_col = the_tuple[1][0]
                        aggr_flag = the_tuple[1][1]

                        # if aggr_flag == 'sum':
                        #     tmp_it_1.iter_op = RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), aggr_col)
                        #
                        #     unopt_context.append(
                        #         LetExpr(varExpr=VarExpr(tmp_vn_nx),
                        #                 valExpr=tmp_it_1.sdql_ir,
                        #                 bodyExpr=ConstantExpr(True))
                        #     )
                        #
                        #     continue
                        # if aggr_flag == 'count':
                        #     tmp_it_1.iter_op = IfExpr(CompareExpr(CompareSymbol.NE,
                        #                                           RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), aggr_col),
                        #                                           ConstantExpr(None)),
                        #                               ConstantExpr(1.0),
                        #                               ConstantExpr(0.0))
                        #
                        #     unopt_context.append(
                        #         LetExpr(varExpr=VarExpr(tmp_vn_nx),
                        #                 valExpr=tmp_it_1.sdql_ir,
                        #                 bodyExpr=ConstantExpr(True))
                        #     )
                        #
                        #     continue
                        if aggr_flag == 'mean':
                            tmp_it_1 = IterForm(tmp_vn_on, tmp_el_on)

                            tmp_it_1.iter_op = IfExpr(
                                CompareExpr(CompareSymbol.NE,
                                            RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), aggr_col),
                                            ConstantExpr(None)),
                                RecConsExpr([
                                    (f'{aggr_col}_sum_for_mean',
                                     RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), aggr_col)),
                                    (f'{aggr_col}_count_for_mean',
                                     ConstantExpr(1.0))]),
                                ConstantExpr(0.0)
                            )

                            unopt_context.append(
                                LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                        valExpr=tmp_it_1.sdql_ir,
                                        bodyExpr=ConstantExpr(True))
                            )

                            unopt_count += 1

                            tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                            tmp_vn_nx = f'{this_name}_{unopt_count}'

                            unopt_context.append(
                                LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                        valExpr=DivExpr(RecAccessExpr(VarExpr(tmp_vn_on_2),
                                                                      f'{aggr_col}_sum_for_mean'),
                                                        RecAccessExpr(VarExpr(tmp_vn_on_2),
                                                                      f'{aggr_col}_count_for_mean')),
                                        bodyExpr=ConstantExpr(True))
                            )

                            continue

                aggr_dict = op_body.aggr_op

                tmp_it_1 = IterForm(tmp_vn_on, tmp_el_on)

                rec_list = []
                for k in aggr_dict.keys():
                    v = aggr_dict[k]
                    if isinstance(v, Replaceable):
                        if v.replaceable:
                            rec_list.append((k, aggr_dict[k].replace(PairAccessExpr(VarExpr(tmp_el_on), 0))))
                        else:
                            rec_list.append((k, aggr_dict[k].sdql_ir))
                    elif isinstance(v, RecAccessExpr):
                        rec_list.append((k, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                          v.name)))
                    else:
                        rec_list.append((k, aggr_dict[k]))

                tmp_it_1.iter_op = RecConsExpr(rec_list)

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it_1.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

                unopt_count += 1

                tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                tmp_vn_nx = f'{this_name}_{unopt_count}'

                if aggr_rename:
                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=DicConsExpr([(
                                    RecConsExpr([(
                                        aggr_rename.rename_to,
                                        RecAccessExpr(VarExpr(tmp_vn_on_2), aggr_rename.rename_from)
                                    )]),
                                    ConstantExpr(True))]),
                                bodyExpr=ConstantExpr(True))
                    )
                else:
                    final_op = DicConsExpr([(VarExpr(tmp_vn_on_2), ConstantExpr(True))])
                    final_op = sr_dict(dict(final_op.initialPairs))

                    unopt_context.append(
                        LetExpr(varExpr=VarExpr(tmp_vn_nx),
                                valExpr=final_op,
                                bodyExpr=ConstantExpr(True))
                    )
            elif isinstance(op_body, AggrOpRename):
                continue
            elif isinstance(op_body, GroupbyAggrExpr):
                has_mean = False

                groupby_cols = op_body.groupby_cols
                aggr_dict = op_body.aggr_dict
                origin_dict = op_body.origin_dict

                nunique_columns = []

                tmp_it_1 = IterForm(tmp_vn_on, tmp_el_on)

                key_rec_list = []
                for c in groupby_cols:
                    key_rec_list.append((c, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                          c)))

                val_rec_list = []
                for k in aggr_dict.keys():
                    if k.endswith('_sum_for_mean') or k.endswith('_count_for_mean'):
                        has_mean = True
                    v = aggr_dict[k]
                    if isinstance(v, Replaceable):
                        if v.replaceable:
                            val_rec_list.append((k, aggr_dict[k].replace(PairAccessExpr(VarExpr(tmp_el_on), 0))))
                        else:
                            val_rec_list.append((k, aggr_dict[k].sdql_ir))
                    elif isinstance(v, RecAccessExpr):
                        # RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), v.name)
                        agg_sum = RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), v.name)
                        # agg_sum = MulExpr(agg_sum, PairAccessExpr(VarExpr(tmp_el_on), 1))
                        val_rec_list.append((k, agg_sum))
                    elif isinstance(v, ConstantExpr):
                        if '_count_for_mean' in k:
                            val_rec_list.append((k, aggr_dict[k]))
                        else:
                            check_count = IfExpr(CompareExpr(CompareSymbol.NE,
                                                             RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                                           origin_dict[k][0]),
                                                             ConstantExpr(None)),
                                                 aggr_dict[k],
                                                 ConstantExpr(0.0))
                            # check_count = MulExpr(check_count, PairAccessExpr(VarExpr(tmp_el_on), 1))
                            val_rec_list.append((k, check_count))
                    elif isinstance(v, AggrNuniqueFunc):
                        # nunique_expr = IfExpr(CompareExpr(CompareSymbol.NE,
                        #                               RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                        #                                             v.field),
                        #                               ConstantExpr(None)),
                        #                   ConstantExpr(1.0),
                        #                   ConstantExpr(0.0))
                        nunique_expr = sr_dict({
                            RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), v.field): ConstantExpr(True)
                        })
                        val_rec_list.append((k, nunique_expr))
                        nunique_columns.append(k)
                    else:
                        val_rec_list.append((k, aggr_dict[k]))

                tmp_it_1.iter_op = DicConsExpr([(RecConsExpr(key_rec_list), RecConsExpr(val_rec_list))])

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it_1.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

                unopt_count += 1

                tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                tmp_vn_nx = f'{this_name}_{unopt_count}'

                tmp_it_2 = IterForm(tmp_vn_on_2, tmp_el_on)

                if has_mean:
                    rec_list_2 = []

                    for i in groupby_cols:
                        rec_list_2.append((i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))

                    origin_dict = op_body.origin_dict
                    for j in origin_dict.keys():
                        if origin_dict[j][1] == 'mean':
                            rec_list_2.append((j, DivExpr(RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 1),
                                                                        f'{j}_sum_for_mean'),
                                                          RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 1),
                                                                        f'{j}_count_for_mean'))))
                        else:
                            rec_list_2.append((j, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 1), j)))

                    tmp_it_2.iter_op = DicConsExpr([(RecConsExpr(rec_list_2),
                                                     ConstantExpr(True))])
                elif nunique_columns:
                    rec_list_2 = []

                    for i in groupby_cols:
                        rec_list_2.append((i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))

                    for u in nunique_columns:
                        rec_list_2.append((u, ExtFuncExpr(ExtFuncSymbol.DictSize,
                                                          RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 1), u),
                                                          ConstantExpr('Nothing!'),
                                                          ConstantExpr('Nothing!'))))

                    tmp_it_2.iter_op = DicConsExpr([(RecConsExpr(rec_list_2),
                                                     ConstantExpr(True))])
                else:
                    tmp_it_2.iter_op = DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr(tmp_el_on), 0),
                                                            PairAccessExpr(VarExpr(tmp_el_on), 1)),
                                                 ConstantExpr(True))])

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it_2.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )
            elif isinstance(op_body, AggrBinOp):
                tmp_vn_on_calc_pre_ops = f'{op_body.descriptor}_pre_ops'

                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                rec_list = []

                single_aggr_dict = self.retriever.find_single_aggr_in_calc(op_body)
                single_aggr_mapper = {}

                for s in single_aggr_dict.keys():
                    sv = single_aggr_dict[s]
                    rec_list.append((s, SDQLInspector.replace_access(sv.sdql_ir, PairAccessExpr(VarExpr(tmp_el_on), 0))))
                    if isinstance(sv.sdql_ir, RecAccessExpr):
                        single_aggr_mapper[sv.sdql_ir.name] = RecAccessExpr(VarExpr(tmp_vn_on_calc_pre_ops), s)
                    else:
                        raise ValueError(f'Unexpected single ir object {sv.sdql_ir}')

                multi_aggr_dict = self.retriever.find_multi_aggr_in_calc(op_body)
                multi_aggr_mapper = {}
                calc_mapper = {}

                for m in multi_aggr_dict.keys():
                    sv = multi_aggr_dict[m]
                    rec_list.append((m, SDQLInspector.replace_access(sv.sdql_ir, PairAccessExpr(VarExpr(tmp_el_on), 0))))
                    multi_aggr_mapper[m] = RecAccessExpr(VarExpr(tmp_vn_on_calc_pre_ops), m)
                    calc_mapper[m] = sv.sdql_ir

                # print('single column', single_aggr_dict)
                # print('multiple columns', multi_aggr_dict)

                tmp_it.iter_op = RecConsExpr(rec_list)

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_on_calc_pre_ops),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

                unopt_count += 1

                tmp_vn_nx = f'{this_name}_{unopt_count}'

                final_mapper = {}

                for k in single_aggr_mapper.keys():
                    final_mapper[k] = single_aggr_mapper[k]

                for k in multi_aggr_mapper.keys():
                    final_mapper[k] = multi_aggr_mapper[k]

                calc_ir = op_body.replace_aggr(calc_mapper, VarExpr(tmp_vn_on_calc_pre_ops)).sdql_ir

                calc_ir = SDQLInspector.replace_field(calc_ir, inplace=True, mapper=final_mapper)

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=calc_ir,
                            bodyExpr=ConstantExpr(True))
                )

                # Method 2
                # tmp_it = IterForm(tmp_vn_on, tmp_el_on)
                #
                # tmp_it.iter_op = SDQLInspector.replace_access(op_body.sdql_ir,
                #                                           PairAccessExpr(VarExpr(tmp_el_on), 0))
                #
                # unopt_context.append(
                #     LetExpr(varExpr=VarExpr(tmp_vn_nx),
                #             valExpr=tmp_it.sdql_ir,
                #             bodyExpr=ConstantExpr(True))
                # )

                # Method 3
                # tmp_it = IterForm(tmp_vn_on, tmp_el_on)
                #
                # rec_list = []
                #
                # for i in SDQLInspector.find_cols(op_body.sdql_ir):
                #     rec_list.append((i, RecAccessExpr(PairAccessExpr(VarExpr(tmp_el_on), 0), i)))
                #
                # tmp_aggr_cols = {}
                #
                # for aggr_expr in self.retriever.split_aggr_in_calc():
                #     for aggr_key in aggr_expr.aggr_op.keys():
                #         target_expr = aggr_expr.aggr_op[aggr_key]
                #         if isinstance(aggr_expr.aggr_op[aggr_key], (AddExpr, MulExpr, SubExpr, DivExpr)):
                #             tmp_aggr_cols[f'{SDQLInspector.find_a_descriptor(target_expr)}'] = target_expr
                #
                # for k in tmp_aggr_cols.keys():
                #     col_op = SDQLInspector.replace_access(tmp_aggr_cols[k],
                #                                           PairAccessExpr(VarExpr(tmp_el_on), 0))
                #     rec_list.append((k, col_op))
                #
                # tmp_it.iter_op = RecConsExpr(rec_list)
                #
                # unopt_context.append(
                #     LetExpr(varExpr=VarExpr(tmp_vn_nx),
                #             valExpr=tmp_it.sdql_ir,
                #             bodyExpr=ConstantExpr(True))
                # )
                #
                # unopt_count += 1
                #
                # tmp_vn_on_2 = f'{this_name}_{unopt_count - 1}'
                # tmp_vn_nx = f'{this_name}_{unopt_count}'
                #
                # calc_ir = op_body.replace_aggr(tmp_aggr_cols, self.opt_on).sdql_ir
                #
                # unopt_context.append(
                #     LetExpr(varExpr=VarExpr(tmp_vn_nx),
                #             valExpr=SDQLInspector.replace_access(calc_ir, VarExpr(tmp_vn_on_2)),
                #             bodyExpr=ConstantExpr(True))
                # )
            elif isinstance(op_body, MergeIndicator):
                continue
            else:
                tmp_it = IterForm(tmp_vn_on, tmp_el_on)

                tmp_it.iter_op = op_body

                unopt_context.append(
                    LetExpr(varExpr=VarExpr(tmp_vn_nx),
                            valExpr=tmp_it.sdql_ir,
                            bodyExpr=ConstantExpr(True))
                )

            unopt_count += 1

        if rename_last:
            if unopt_context:
                unopt_context[-1] = LetExpr(VarExpr(rename_last),
                                            unopt_context[-1].valExpr,
                                            unopt_context[-1].bodyExpr)

        # print(col_attach_cache)

        return unopt_context

    def get_unopt_sdqlir(self, rename_last='', as_result=True):
        all_unopt = self.get_unopt_context()

        if as_result:
            all_unopt[-1] = SDQLInspector.rename_last_binding(all_unopt[-1],
                                                              f'results',
                                                              with_res=False)
        else:
            if rename_last:
                all_unopt[-1] = SDQLInspector.rename_last_binding(all_unopt[-1],
                                                                  rename_last,
                                                                  with_res=False)

        return SDQLInspector.concat_bindings(all_unopt, drop_duplicates=True)
