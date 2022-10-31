from pysdql.core.dtypes.JoinPartitionFrame import JoinPartitionFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr

from pysdql.core.dtypes.sdql_ir import (
    SumExpr, IfExpr, VarExpr, ConstantExpr, LetExpr, DicConsExpr, RecConsExpr, SumBuilder, ConcatExpr, MulExpr,
    DicLookupExpr, CompareExpr, CompareSymbol, EmptyDicConsExpr, RecAccessExpr,
)


class JointFrame:
    def __init__(self, partition: JoinPartitionFrame, probe: JoinProbeFrame, joint, col_ins=None, col_proj=None,
                 groupby_cols=None, aggr_dict=None, joint_cond=None):
        """

        :param partition:
        :param probe:
        :param joint:
        :param col_ins:
        :param col_proj:
        :param groupby_cols: list
        :param aggr_dict: dict
        :param joint_cond: CondExpr
        """
        self.__partition_frame = partition
        self.__probe_frame = probe
        self.__op = None
        self.__next_op = None
        self.__joint = joint
        self.__col_ins = col_ins if col_ins else {}
        self.__col_proj = col_proj if col_proj else []
        self.groupby_cols = groupby_cols if groupby_cols else []
        self.aggr_dict = aggr_dict if aggr_dict else {}
        self.joint_cond = joint_cond

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
    def part_frame(self):
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

        # df: partition side
        part_on = self.part_frame.get_partition_on()
        # var:
        part_var = self.part_frame.get_part_var()
        this_part_col_proj = self.part_frame.get_part_col_proj()

        # df: probe side
        probe_on = self.probe_frame.get_probe_on()
        # var: joint dict
        probe_var = self.joint.var_expr

        # str: right_on
        probe_key = self.probe_frame.get_probe_key()
        probe_key_ir = probe_on.key_access(probe_key)

        probe_cond = self.probe_frame.get_probe_cond()
        this_probe_col_proj = self.probe_frame.get_probe_col_proj()

        joint_cond = self.joint_cond
        joint_col_proj = self.col_proj
        joint_col_proj_ir = RecConsExpr(self.col_proj) if self.col_proj else RecConsExpr([(probe_key,
                                                                                           probe_on.key_access(
                                                                                               probe_key))])

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

            if joint_cond:
                joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                    leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                           keyExpr=probe_key_ir),
                                                                    rightExpr=ConstantExpr(None)),
                                               thenBodyExpr=IfExpr(condExpr=joint_cond,
                                                                   thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                                              aggr_val_ir)]),
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

            if probe_cond:
                joint_groupby_aggr_op = IfExpr(condExpr=probe_cond,
                                               thenBodyExpr=joint_groupby_aggr_op,
                                               elseBodyExpr=EmptyDicConsExpr())

            # else:
            #     joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
            #                                                         leftExpr=DicLookupExpr(dicExpr=part_var,
            #                                                                                keyExpr=probe_key_ir),
            #                                                         rightExpr=ConstantExpr(None)),
            #                                    thenBodyExpr=DicConsExpr([(aggr_key_ir,
            #                                                               aggr_val_ir
            #                                                               )]),
            #                                    elseBodyExpr=EmptyDicConsExpr())

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

            if joint_cond:
                part_side = self.partition_frame.partition_on
                next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                     on=part_side)
                joint_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                            leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                   keyExpr=probe_key_ir),
                                                            rightExpr=ConstantExpr(None)),
                                       thenBodyExpr=IfExpr(condExpr=next_joint_cond,
                                                           thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                                      ConstantExpr(True))]),
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

            if probe_cond:
                joint_aggr_op = IfExpr(condExpr=probe_cond,
                                       thenBodyExpr=joint_aggr_op,
                                       elseBodyExpr=EmptyDicConsExpr())

            # if probe_cond:
            #     joint_aggr_op = IfExpr(condExpr=probe_cond,
            #                            thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
            #                                                                     leftExpr=DicLookupExpr(
            #                                                                         dicExpr=part_var,
            #                                                                         keyExpr=probe_key_ir),
            #                                                                     rightExpr=ConstantExpr(None)),
            #                                                thenBodyExpr=DicConsExpr([(
            #                                                    aggr_key_ir,
            #                                                    ConstantExpr(True)
            #                                                )]),
            #                                                elseBodyExpr=EmptyDicConsExpr()),
            #                            elseBodyExpr=EmptyDicConsExpr())
            # else:
            #     joint_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
            #                                                 leftExpr=DicLookupExpr(dicExpr=part_var,
            #                                                                        keyExpr=probe_key_ir),
            #                                                 rightExpr=ConstantExpr(None)),
            #                            thenBodyExpr=DicConsExpr([(aggr_key_ir,
            #                                                       ConstantExpr(True)
            #                                                       )]),
            #                            elseBodyExpr=EmptyDicConsExpr())

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_aggr_op,
                               isAssignmentSum=False)

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=LetExpr(VarExpr('out'), probe_var, ConstantExpr(True)))

            return output
        else:
            if self.is_next_probe:
                probe_cols = probe_on.columns
                # this partition
                this_part_var = self.part_frame.get_part_var()
                this_part_key = self.part_frame.get_part_key()
                this_part_cols = self.part_frame.get_partition_on().columns

                # this probe
                this_probe_key = probe_key
                this_probe_key_ir = probe_on.key_access(this_probe_key)

                # next merge
                next_merge = self.get_next_merge_expr()

                # next partition
                next_part_frame = self.get_next_part_frame()
                next_part_col_proj = next_part_frame.get_part_col_proj()
                next_part_var = next_part_frame.get_part_var()
                next_part_key = next_merge.left_on
                next_part_cols = next_part_frame.get_partition_on().columns

                # next probe

                next_probe_key = next_merge.right_on

                iter_key = probe_key

                for left_key, right_key in probe_on.find_cols_as_key_tuple():
                    if left_key in probe_cols:
                        iter_key = left_key

                iter_key_ir = probe_on.key_access(iter_key)

                # print(f'We need a probe here: \n'
                #       f'probe side: {probe_on.name}\n'
                #       f'1st part_side: {this_part_var}\n'
                #       f'2nd part_side: {next_part_var}\n'
                #       f'joint: {self.joint.name}')

                # print(f'''
                # this_part_var: {this_part_var}
                # this_part_key: {this_part_key}
                # this_part_cols: {this_part_cols}
                #
                # this_probe_key: {this_probe_key}
                #
                # next_part_cols: {next_part_cols}
                # next_part_var: {next_part_var}
                # next_part_key: {next_part_key}
                #
                # next_probe_key: {next_probe_key}

                # {probe_on.find_cols_as_key_tuple()}
                # ''')

                val_rec_list = []

                if this_part_col_proj:
                    for col in this_part_col_proj:
                        field = col[0]
                        if field != next_probe_key:
                            if field in this_part_cols:
                                val_rec_list.append((field, RecAccessExpr(recExpr=DicLookupExpr(dicExpr=this_part_var,
                                                                                                keyExpr=this_probe_key_ir),
                                                                          fieldName=field)))
                if next_part_col_proj:
                    for col in next_part_col_proj:
                        field = col[0]
                        if field != next_part_key:
                            if next_probe_key not in probe_cols:
                                val_rec_list.append((field,
                                                     RecAccessExpr(recExpr=DicLookupExpr(dicExpr=next_part_var,
                                                                                         keyExpr=RecAccessExpr(
                                                                                             recExpr=DicLookupExpr(
                                                                                                 dicExpr=this_part_var,
                                                                                                 keyExpr=this_probe_key_ir),
                                                                                             fieldName=next_probe_key)),
                                                                   fieldName=field)))
                            else:
                                raise NotImplementedError
                if this_probe_col_proj:
                    raise NotImplementedError
                else:
                    next_col_proj_ir = RecConsExpr([(iter_key, iter_key_ir)])

                iter_val_ir = RecConsExpr(val_rec_list)

                if joint_cond:
                    part_side = self.partition_frame.partition_on
                    next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                         on=part_side).sdql_ir

                    joint_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                           leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                  keyExpr=probe_key_ir),
                                                           rightExpr=ConstantExpr(None)),
                                      thenBodyExpr=IfExpr(condExpr=next_joint_cond,
                                                          thenBodyExpr=DicConsExpr([(iter_key_ir,
                                                                                     iter_val_ir)]),
                                                          elseBodyExpr=EmptyDicConsExpr()),
                                      elseBodyExpr=EmptyDicConsExpr())
                else:
                    joint_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                           leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                  keyExpr=probe_key_ir),
                                                           rightExpr=ConstantExpr(None)),
                                      thenBodyExpr=DicConsExpr([(
                                          iter_key_ir,
                                          iter_val_ir
                                      )]),
                                      elseBodyExpr=EmptyDicConsExpr())
                if probe_cond:
                    joint_op = IfExpr(condExpr=probe_cond,
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=EmptyDicConsExpr())

                output = LetExpr(varExpr=probe_var,
                                 valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                                 dictExpr=probe_on.var_expr,
                                                 bodyExpr=joint_op,
                                                 isAssignmentSum=False),
                                 bodyExpr=next_op)

                return output
            if self.is_next_part:
                iter_key = self.get_next_part_key()
                next_part_key_ir = probe_on.key_access(iter_key)
                if self.col_proj:
                    next_col_proj = [col for col in self.col_proj if col[0] != iter_key]
                    next_col_proj_ir = RecConsExpr(next_col_proj)
                else:
                    next_col_proj_ir = RecConsExpr([(iter_key, next_part_key_ir)])

                if joint_cond:
                    part_side = self.partition_frame.partition_on
                    next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                         on=part_side).sdql_ir

                    joint_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                           leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                  keyExpr=probe_key_ir),
                                                           rightExpr=ConstantExpr(None)),
                                      thenBodyExpr=IfExpr(condExpr=next_joint_cond,
                                                          thenBodyExpr=DicConsExpr([(next_part_key_ir,
                                                                                     next_col_proj_ir)]),
                                                          elseBodyExpr=EmptyDicConsExpr()),
                                      elseBodyExpr=EmptyDicConsExpr())
                else:
                    joint_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                           leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                  keyExpr=probe_key_ir),
                                                           rightExpr=ConstantExpr(None)),
                                      thenBodyExpr=DicConsExpr([(
                                          next_part_key_ir,
                                          next_col_proj_ir
                                      )]),
                                      elseBodyExpr=EmptyDicConsExpr())
                if probe_cond:
                    joint_op = IfExpr(condExpr=probe_cond,
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=EmptyDicConsExpr())

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

        if not self.partition_frame.is_joint and not self.probe_frame.is_joint:
            print(f'{self.joint.name}: neither joint')
            return self.partition_frame.get_part_expr(self.get_probe_expr(next_op))
        if self.partition_frame.is_joint and not self.probe_frame.is_joint:
            print(f'{self.joint.name}: part joint')
            return self.partition_frame.partition_on.get_joint_frame().get_joint_expr(next_op)
        if not self.partition_frame.is_joint and self.probe_frame.is_joint:
            print(f'{self.joint.name}: probe joint')
            # return self.get_next_part_frame().get_part_expr(self.part_frame.get_part_expr(self.probe_frame.probe_on.get_joint_frame().get_joint_expr(next_op)))
            return self.part_frame.get_part_expr(self.probe_frame.probe_on.get_joint_frame().get_joint_expr(next_op))
        raise NotImplemented

    @property
    def is_first_joint(self):
        if not self.partition_frame.is_joint and not self.probe_frame.is_joint:
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

    def get_next_merge_expr(self):
        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if self.joint.name == op_expr.op.left.name or self.joint.name == op_expr.op.right.name:
                    return op_expr.op
                if self.joint.name != op_expr.op.joint.name:
                    return op_expr.op
        else:
            raise ValueError()

    def get_next_part_frame(self) -> JoinPartitionFrame:
        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if self.joint.name == op_expr.op.left.name or self.joint.name == op_expr.op.right.name:
                    return op_expr.op.left.get_partition_frame()
        else:
            raise ValueError()

    def get_next_part_key(self) -> str:
        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if self.joint.name == op_expr.op.left.name or self.joint.name == op_expr.op.right.name:
                    return op_expr.op.left_on
        else:
            raise ValueError()

    @property
    def is_next_part(self):
        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if self.joint.name == op_expr.op.left.name:
                    return True
        return False

    @property
    def is_next_probe(self):
        for op_expr in self.joint.operations:
            if op_expr.op_type == MergeExpr:
                if self.joint.name == op_expr.op.right.name:
                    return True
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
