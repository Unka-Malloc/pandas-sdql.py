from pysdql.core.dtypes.JoinPartitionFrame import JoinPartitionFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr

from pysdql.core.dtypes.sdql_ir import (
    SumExpr, IfExpr, VarExpr, ConstantExpr, LetExpr, DicConsExpr, RecConsExpr, SumBuilder, ConcatExpr, MulExpr,
    DicLookupExpr, CompareExpr, CompareSymbol, EmptyDicConsExpr, RecAccessExpr,
)


class JointFrame:
    def __init__(self, partition: JoinPartitionFrame, probe: JoinProbeFrame, joint, col_ins=None, col_proj=None,
                 groupby_cols=None,
                 aggr_dict=None, ):
        self.__partition_frame = partition
        self.__probe_frame = probe
        self.__op = None
        self.__next_op = None
        self.__joint = joint
        self.__col_ins = col_ins if col_ins else {}
        self.__col_proj = col_proj if col_proj else []
        self.groupby_cols = groupby_cols if groupby_cols else []
        self.aggr_dict = aggr_dict if aggr_dict else {}

    @property
    def joint(self):
        return self.__joint

    @property
    def col_proj(self):
        return self.__col_proj

    @property
    def col_ins(self):
        return self.__col_ins

    def add_col_proj(self, val):
        self.__col_proj = val

    @property
    def partition_frame(self):
        return self.__partition_frame

    @property
    def probe_frame(self):
        return self.__probe_frame

    def add_op(self, val):
        self.__op = val

    def add_next(self, val):
        self.__next_op = val

    @property
    def var_part(self):
        return self.__partition_frame.part_var

    @property
    def var_probe(self):
        return self.__probe_frame.var_probe

    def add_col_proj(self, val):
        self.__probe_frame.add_col_proj(val)

    def get_probe_expr(self, next_op=None):
        if not next_op:
            if self.__next_op:
                next_op = self.__next_op
            else:
                next_op = ConstantExpr('placeholder_probe_next')

        # str: right_on
        probe_key = self.probe_frame.get_probe_key()
        probe_cond = self.probe_frame.get_probe_cond()
        # var:
        part_var = self.partition_frame.get_part_var()
        # var: joint dict
        probe_var = self.joint.var_expr
        # df: probe side
        probe_on = self.probe_frame.get_probe_on()

        probe_key_ir = probe_on.key_access(probe_key)

        col_proj_ir = RecConsExpr(self.col_proj) if self.col_proj else RecConsExpr([(probe_key,
                                                                                     probe_on.key_access(probe_key))])

        if self.is_groupby_agg_joint:
            # aggr_key_ir
            key_rec_list = []
            for i in self.groupby_cols:
                if i == probe_key:
                    key_rec_list.append((i, probe_on.key_access(i)))
                if i in self.partition_frame.cols_out:
                    key_rec_list.append((i,
                                         RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                             keyExpr=probe_on.key_access(i)),
                                                       fieldName=i)))
            aggr_key_ir = RecConsExpr(key_rec_list)

            # aggr_val_ir
            val_rec_list = []
            if self.col_ins:
                for k in self.aggr_dict.keys():
                    v = self.aggr_dict[k]
                    if v.name in self.col_ins.keys():
                        col_expr = self.col_ins[v.name].sdql_ir
                    else:
                        col_expr = v
                    val_rec_list.append((k, col_expr))
            else:
                for k in self.aggr_dict.keys():
                    val_rec_list.append((k, self.aggr_dict[k]))
            aggr_val_ir = RecConsExpr(val_rec_list)

            if probe_cond:
                joint_groupby_aggr_op = IfExpr(condExpr=probe_cond,
                                               thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                                        leftExpr=DicLookupExpr(
                                                                                            dicExpr=part_var,
                                                                                            keyExpr=probe_key_ir),
                                                                                        rightExpr=ConstantExpr(None)),
                                                                   thenBodyExpr=DicConsExpr([(
                                                                       aggr_key_ir,
                                                                       aggr_val_ir
                                                                   )]),
                                                                   elseBodyExpr=EmptyDicConsExpr()),
                                               elseBodyExpr=EmptyDicConsExpr())
            else:
                joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                    leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                           keyExpr=probe_key_ir),
                                                                    rightExpr=ConstantExpr(None)),
                                               thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                          aggr_val_ir
                                                                          )]),
                                               elseBodyExpr=EmptyDicConsExpr())

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_groupby_aggr_op,
                               isAssignmentSum=False)

            result = VarExpr('result')

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=LetExpr(varExpr=result,
                                              valExpr=SumBuilder(
                                                  lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]),
                                                  probe_var,
                                                  True),
                                              bodyExpr=LetExpr(VarExpr('out'), result, ConstantExpr(True))))

            return output
        elif self.is_agg_joint:
            # aggr_key_ir
            val_rec_list = []
            if self.col_ins:
                for k in self.aggr_dict.keys():
                    v = self.aggr_dict[k]
                    if v in self.col_ins.keys():
                        col_expr = self.col_ins[v].sdql_ir
                    else:
                        col_expr = v
                    val_rec_list.append((k, col_expr))
            else:
                for k in self.aggr_dict.keys():
                    val_rec_list.append((k, self.aggr_dict[k]))
            aggr_key_ir = RecConsExpr(val_rec_list)

            if probe_cond:
                joint_aggr_op = IfExpr(condExpr=probe_cond,
                                               thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                                        leftExpr=DicLookupExpr(
                                                                                            dicExpr=part_var,
                                                                                            keyExpr=probe_key_ir),
                                                                                        rightExpr=ConstantExpr(None)),
                                                                   thenBodyExpr=DicConsExpr([(
                                                                       aggr_key_ir,
                                                                       ConstantExpr(True)
                                                                   )]),
                                                                   elseBodyExpr=EmptyDicConsExpr()),
                                               elseBodyExpr=EmptyDicConsExpr())
            else:
                joint_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                    leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                           keyExpr=probe_key_ir),
                                                                    rightExpr=ConstantExpr(None)),
                                               thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                          ConstantExpr(True)
                                                                          )]),
                                               elseBodyExpr=EmptyDicConsExpr())

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_aggr_op,
                               isAssignmentSum=False)

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=LetExpr(VarExpr('out'), probe_var, ConstantExpr(True)))

            return output
        else:
            if self.is_passby:
                next_part_key = self.get_next_part_key()
                next_part_key_ir = probe_on.key_access(next_part_key)
                if self.col_proj:
                    next_col_proj = [col for col in self.col_proj if col[0] != next_part_key]
                    next_col_proj_ir = RecConsExpr(next_col_proj)
                else:
                    next_col_proj_ir = RecConsExpr([(next_part_key, next_part_key_ir)])

                inner_expr = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                         leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                keyExpr=probe_key_ir),
                                                         rightExpr=ConstantExpr(None)),
                                    thenBodyExpr=DicConsExpr([(
                                        next_part_key_ir,
                                        next_col_proj_ir
                                    )]),
                                    elseBodyExpr=EmptyDicConsExpr())
            else:
                inner_expr = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                         leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                keyExpr=probe_key_ir),
                                                         rightExpr=ConstantExpr(None)),
                                    thenBodyExpr=DicConsExpr([(
                                        probe_key_ir,
                                        col_proj_ir
                                    )]),
                                    elseBodyExpr=EmptyDicConsExpr())
            if probe_cond:
                joint_op = IfExpr(condExpr=probe_cond,
                                  thenBodyExpr=inner_expr,
                                  elseBodyExpr=EmptyDicConsExpr())
            else:
                joint_op = inner_expr

            output = LetExpr(varExpr=probe_var,
                             valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                             dictExpr=probe_on.var_expr,
                                             bodyExpr=joint_op,
                                             isAssignmentSum=False),
                             bodyExpr=next_op)

            return output

    def get_joint_expr(self, next_op=None):
        if not next_op:
            if self.is_groupby_agg_joint:
                next_op = self.get_probe_expr()
            else:
                next_op = ConstantExpr('placeholder_probe_next')

        if self.is_first_joint:
            return self.partition_frame.get_part_expr(self.get_probe_expr(next_op))
        else:
            if self.partition_frame.is_joint:
                return self.partition_frame.partition_on.get_joint_frame().get_joint_expr(next_op)
            else:
                raise ValueError()

    @property
    def is_first_joint(self):
        if not self.partition_frame.is_joint and not self.partition_frame.is_joint:
            return True
        else:
            return False

    @property
    def sdql_ir(self):
        return self.get_joint_expr()

    @property
    def is_groupby_agg_joint(self):
        if self.groupby_cols and self.aggr_dict:
            return True
        else:
            return False

    @property
    def is_agg_joint(self):
        if self.aggr_dict:
            return True
        else:
            return False

    def get_next_part_key(self):
        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if self.joint.name == op_expr.op.left.name:
                    return op_expr.op.left_on

    @property
    def is_passby(self):
        probe_on = self.probe_frame.get_probe_on()

        was_last_probe = False
        is_next_part = False

        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if probe_on.name == op_expr.op.right.name:
                    was_last_probe = True
                if self.joint.name == op_expr.op.left.name:
                    is_next_part = True

        if was_last_probe and is_next_part:
            return True
        else:
            return False

    def __repr__(self):
        return f'''
        >> Joint Frame ({self.__joint.name}) <<
            ColProj: {self.__col_proj}
            GroupBy: {self.groupby_cols}
            AggrDict: {self.aggr_dict}
        
        >> Partition Frame ({self.__joint.name}):
                {self.__partition_frame}
        >> Probe Frame ({self.__joint.name}): 
            {self.__probe_frame}
        '''
