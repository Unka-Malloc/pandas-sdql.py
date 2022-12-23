from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.JoinPartitionFrame import JoinPartitionFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr

from pysdql.core.dtypes.sdql_ir import *


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
        # Q3 -> this way, sir
        if self.is_groupby_agg_joint:
            # aggr_key_ir
            key_rec_list = []
            for i in self.groupby_cols:
                if i == probe_key:
                    key_rec_list.append((i, probe_on.key_access(i)))
                if i in self.partition_frame.cols_out:
                    key_rec_list.append((i,
                                         RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                             keyExpr=probe_on.key_access(probe_key)),
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
                last_cond = self.probe_frame.get_cond_after_groupby_agg()
                if not last_cond:
                    joint_groupby_aggr_op = IfExpr(condExpr=probe_cond,
                                                   thenBodyExpr=joint_groupby_aggr_op,
                                                   elseBodyExpr=EmptyDicConsExpr())

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_groupby_aggr_op,
                               isAssignmentSum=False)

            vname_concat = f'x_{self.joint.name}'
            var_concat = VarExpr(vname_concat)
            self.joint.add_context_variable(vname_concat, var_concat)
            sum_concat = SumExpr(varExpr=var_concat,
                                 dictExpr=probe_var,
                                 bodyExpr=DicConsExpr([(ConcatExpr(PairAccessExpr(var_concat, 0),
                                                                   PairAccessExpr(var_concat, 1)),
                                                        ConstantExpr(True))]),
                                 isAssignmentSum=True)

            var_out = VarExpr('out')
            self.joint.add_context_variable('out', var_out)

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=LetExpr(var_out, sum_concat, ConstantExpr(True)))

            return output
        elif self.is_agg_joint:
            if len(self.aggr_dict.keys()) == 1:
                aggr_dict_tuple_list = []
                for k in self.aggr_dict.keys():
                    v = self.aggr_dict[k]
                    if self.col_ins:
                        if v in self.col_ins.keys():
                            col_expr = self.col_ins[v].sdql_ir
                        else:
                            col_expr = v
                        aggr_dict_tuple_list.append((k, col_expr))
                    else:
                        aggr_dict_tuple_list.append((k, v))

                aggr_ir = DicConsExpr(aggr_dict_tuple_list)
            else:
                raise NotImplementedError

            if joint_cond:
                part_side = self.partition_frame.partition_on
                next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                     on=part_side)

                joint_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                            leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                   keyExpr=probe_key_ir),
                                                            rightExpr=ConstantExpr(None)),
                                       thenBodyExpr=IfExpr(condExpr=next_joint_cond,
                                                           thenBodyExpr=aggr_ir,
                                                           elseBodyExpr=EmptyDicConsExpr()),
                                       elseBodyExpr=EmptyDicConsExpr())
            else:
                joint_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                            leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                   keyExpr=probe_key_ir),
                                                            rightExpr=ConstantExpr(None)),
                                       thenBodyExpr=aggr_ir,
                                       elseBodyExpr=EmptyDicConsExpr())

            if probe_cond:
                joint_aggr_op = IfExpr(condExpr=probe_cond,
                                       thenBodyExpr=joint_aggr_op,
                                       elseBodyExpr=EmptyDicConsExpr())

            vname_concat = f'x_{self.joint.name}'
            var_concat = VarExpr(vname_concat)
            self.joint.add_context_variable(vname_concat, var_concat)
            sum_concat = SumExpr(varExpr=var_concat,
                                 dictExpr=probe_var,
                                 bodyExpr=DicConsExpr([(ConcatExpr(PairAccessExpr(var_concat, 0),
                                                                   PairAccessExpr(var_concat, 1)),
                                                        ConstantExpr(True))]),
                                 isAssignmentSum=True)

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_aggr_op,
                               isAssignmentSum=False)

            var_out = VarExpr('out')
            self.joint.add_context_variable('out', var_out)

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=LetExpr(var_out, sum_concat, ConstantExpr(True)))

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
            # Q3 -> this way, sir
            # Q18 -> this way, sir
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

                isin_expr = self.probe_frame.get_isin()
                if isin_expr:
                    vname_having = f'{isin_expr.part_on.name}_having'
                    var_having = isin_expr.part_on.context_variable[vname_having]
                    joint_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                           DicLookupExpr(var_having,
                                                                         probe_on.key_access(
                                                                             isin_expr.col_probe.field)),
                                                           ConstantExpr(None)),
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=EmptyDicConsExpr()
                                      )

                output = LetExpr(varExpr=probe_var,
                                 valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                                 dictExpr=probe_on.var_expr,
                                                 bodyExpr=joint_op,
                                                 isAssignmentSum=True),
                                 bodyExpr=next_op)

                return output

            if self.probe_frame.was_groupby_agg:
                var_part = self.probe_frame.probe_on.get_var_part()

                x_vname_part = f'x_{var_part.name}'
                x_var_part = VarExpr(x_vname_part)
                self.joint.add_context_variable(x_vname_part, x_var_part)

                iter_key_list = []
                for col in self.col_proj:
                    cname = col[0]
                    if cname in self.part_frame.partition_on.columns:
                        if cname == self.part_frame.get_part_key():
                            iter_key_list.append(
                                (cname, RecAccessExpr(PairAccessExpr(x_var_part, 0),
                                                      self.probe_frame.get_probe_key())))
                        else:
                            iter_key_list.append((cname, RecAccessExpr(DicLookupExpr(self.part_frame.get_part_var(),
                                                                                     RecAccessExpr(PairAccessExpr(
                                                                                         x_var_part,
                                                                                         0),
                                                                                         self.probe_frame.get_probe_key())),
                                                                       cname)))
                    if cname in self.probe_frame.get_aggr_dict().keys():
                        iter_key_list.append(
                            (cname, RecAccessExpr(PairAccessExpr(x_var_part, 1),
                                                  cname)))

                new_iter_key_list = []
                for i in iter_key_list:
                    if i not in new_iter_key_list:
                        new_iter_key_list.append(i)

                last_op = DicConsExpr([(RecConsExpr(new_iter_key_list), ConstantExpr(True))])

                last_cond = self.probe_frame.get_cond_after_groupby_agg()

                if last_cond:
                    last_op = IfExpr(last_cond.replace(PairAccessExpr(x_var_part,
                                                                      1)),
                                     last_op,
                                     EmptyDicConsExpr())

                sum_expr = SumExpr(varExpr=x_var_part,
                                   dictExpr=var_part,
                                   bodyExpr=last_op,
                                   isAssignmentSum=True)

                var_out = VarExpr('out')
                self.joint.add_context_variable('out', var_out)

                output = LetExpr(varExpr=var_out,
                                 valExpr=sum_expr,
                                 bodyExpr=ConstantExpr(True))

                return output

            if self.probe_frame.has_isin():
                # aggr_key_ir
                key_rec_list = []
                for i in self.groupby_cols:
                    if i == probe_key:
                        key_rec_list.append((i, probe_on.key_access(i)))
                    if i in self.partition_frame.cols_out:
                        key_rec_list.append((i,
                                             RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                 keyExpr=probe_on.key_access(
                                                                                     probe_key)),
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

                joint_groupby_aggr_op = DicConsExpr([(aggr_key_ir,
                                                      aggr_val_ir
                                                      )])

                if joint_cond:
                    joint_groupby_aggr_op = IfExpr(condExpr=joint_cond,
                                                   thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                              aggr_val_ir)]),
                                                   elseBodyExpr=EmptyDicConsExpr())

                isin_expr = self.probe_frame.get_isin()
                if isin_expr:
                    if isin_expr.isinvert:
                        joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.EQ,
                                                                            DicLookupExpr(isin_expr.part_on.var_part,
                                                                                          probe_on.key_access(
                                                                                              isin_expr.col_probe.field)),
                                                                            ConstantExpr(None)),
                                                       thenBodyExpr=joint_groupby_aggr_op,
                                                       elseBodyExpr=EmptyDicConsExpr()
                                                       )
                    else:
                        joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                            DicLookupExpr(isin_expr.part_on.var_part,
                                                                                          probe_on.key_access(
                                                                                              isin_expr.col_probe.field)),
                                                                            ConstantExpr(None)),
                                                       thenBodyExpr=joint_groupby_aggr_op,
                                                       elseBodyExpr=EmptyDicConsExpr()
                                                       )

                joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                    leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                           keyExpr=probe_key_ir),
                                                                    rightExpr=ConstantExpr(None)),
                                               thenBodyExpr=joint_groupby_aggr_op,
                                               elseBodyExpr=EmptyDicConsExpr())

                sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                   dictExpr=probe_on.var_expr,
                                   bodyExpr=joint_groupby_aggr_op,
                                   isAssignmentSum=False)

                vname_concat = f'x_{self.joint.name}'
                var_concat = VarExpr(vname_concat)
                self.joint.add_context_variable(vname_concat, var_concat)
                sum_concat = SumExpr(varExpr=var_concat,
                                     dictExpr=probe_var,
                                     bodyExpr=DicConsExpr([(ConcatExpr(PairAccessExpr(var_concat, 0),
                                                                       PairAccessExpr(var_concat, 1)),
                                                            ConstantExpr(True))]),
                                     isAssignmentSum=True)

                var_out = VarExpr('out')
                self.joint.add_context_variable('out', var_out)

                output = LetExpr(varExpr=probe_var,
                                 valExpr=sum_expr,
                                 bodyExpr=LetExpr(var_out, sum_concat, ConstantExpr(True)))

                return output
            # Q14 -> this way
            if self.is_next_calc:
                rec_list = []
                for i in self.col_ins:
                    if isinstance(self.col_ins[i], IfExpr):
                        prev_if = self.col_ins[i]
                        next_if = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                              DicLookupExpr(self.part_frame.part_var,
                                                                            probe_key_ir),
                                                              ConstantExpr(None)),
                                         thenBodyExpr=prev_if.thenBodyExpr,
                                         elseBodyExpr=prev_if.elseBodyExpr)
                        rec_list.append((i, next_if))
                    else:
                        rec_list.append((i, self.col_ins[i]))
                rec = RecConsExpr(rec_list)

                cond = self.probe_frame.get_probe_cond()
                if cond:
                    rec = IfExpr(condExpr=cond,
                                 thenBodyExpr=rec,
                                 elseBodyExpr=ConstantExpr(None))

                sum_expr = SumExpr(varExpr=self.probe_frame.get_probe_on().iter_el.el,
                                   dictExpr=self.probe_frame.get_probe_on_var(),
                                   bodyExpr=rec,
                                   isAssignmentSum=False)

                calc_expr = self.get_next_calc()

                var_out = VarExpr('out')
                self.joint.add_context_variable('out', var_out)

                output = LetExpr(varExpr=probe_var,
                                 valExpr=sum_expr,
                                 bodyExpr=LetExpr(var_out, calc_expr, ConstantExpr(True)))

                return output

                # print(self.col_ins)
                # print(self.probe_frame.get_probe_cond())
                # print(self.get_next_calc().sdql_ir)

    def get_joint_expr(self, next_op=None):
        if not next_op:
            if self.is_groupby_agg_joint:
                next_op = self.get_probe_expr()
            else:
                next_op = ConstantExpr('placeholder_probe_next')

        # print(self.partition_frame.is_joint, self.probe_frame.is_joint)
        # print(self.part_frame.partition_on.name, self.probe_frame.probe_on.name)

        # Q15 -> this way, sir
        # Q16 -> this way, sir
        # Q19 -> this way, sir
        if not self.partition_frame.is_joint and not self.probe_frame.is_joint:
            isin_expr = self.probe_frame.get_isin()
            if isin_expr:
                return self.probe_frame.probe_on.get_having(
                    self.partition_frame.get_part_expr(self.get_probe_expr(next_op)))
            if self.probe_frame.was_groupby_agg:
                tmp_let_expr = self.probe_frame.probe_on.get_groupby_agg()
                tmp_let_expr = LetExpr(self.probe_frame.get_probe_on().get_var_part(),
                                       tmp_let_expr.valExpr,
                                       self.part_frame.get_part_expr(self.get_probe_expr(next_op)))
                return tmp_let_expr

            # print(f'{self.joint.name}: neither joint')
            return self.partition_frame.get_part_expr(self.get_probe_expr(next_op))
        if self.partition_frame.is_joint and not self.probe_frame.is_joint:
            # print(f'{self.joint.name}: part joint')
            return self.partition_frame.partition_on.get_joint_frame().get_joint_expr(next_op)
        if not self.partition_frame.is_joint and self.probe_frame.is_joint:
            # print(f'{self.joint.name}: probe joint')
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

    @property
    def is_next_calc(self):
        for op_expr in self.joint.operations:
            if op_expr.op_type == CalcExpr:
                return True
        return False

    def get_next_calc(self):
        for op_expr in self.joint.operations:
            if op_expr.op_type == CalcExpr:
                return op_expr.op
        return None

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
