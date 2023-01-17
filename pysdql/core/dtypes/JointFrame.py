from pysdql.core.dtypes import ColExpr, ColEl, ExternalExpr
from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import OpRetType
from pysdql.core.dtypes.JoinPartFrame import JoinPartFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr
from pysdql.core.dtypes.SDQLInspector import SDQLInspector

from pysdql.core.dtypes.sdql_ir import *
from pysdql.core.util.df_retriever import Retriever


class JointFrame:
    def __init__(self,
                 partition: JoinPartFrame,
                 probe: JoinProbeFrame,
                 joint,
                 col_ins=None,
                 col_proj=None,
                 groupby_cols=None,
                 aggr_dict=None,
                 joint_cond=None):
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
        self.__part_frame = partition
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
    def retriever(self) -> Retriever:
        return self.joint.get_retriever()

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
    def partition_frame(self) -> JoinPartFrame:
        return self.__partition_frame

    @property
    def part_frame(self) -> JoinPartFrame:
        return self.__part_frame

    @property
    def probe_frame(self) -> JoinProbeFrame:
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
        return self.probe_frame.get_probe_on_var()

    def probe_access(self, col_name):
        return self.probe_frame.probe_on.key_access(col_name)

    def part_lookup(self, col_name):
        return RecAccessExpr(recExpr=DicLookupExpr(dicExpr=self.part_frame.part_var,
                                                   keyExpr=self.probe_frame.probe_on.key_access(
                                                       self.probe_frame.probe_key)),
                             fieldName=col_name)

    def get_probe_expr(self, next_op=None):
        """
        Precondition for calling this function:
            The partition side is never generated.

        I understand it is very confusing. This program works only if the probe process finished before the partition starts.
        This conflicts with partition-probe hash join.
        We have to assume the partition looks like ...
        Then the probe starts based on this assumption.
        Finally, we put it together and hope partition just work as we hope.
        :param next_op:
        :return:
        """
        if not next_op:
            if self.__next_op:
                next_op = self.__next_op
            else:
                next_op = ConstantExpr('placeholder_probe_next')

        # df: partition side
        part_on = self.part_frame.get_part_on()
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

        if self.retriever.is_last_joint():
            if self.retriever.last_iter_is_groupby_agg:
                if self.probe_frame.retriever.is_joint:
                    # If the probe side is joint, then there must be multiple partitions
                    # Therefore, we need to probe on every partition

                    all_part_sides = self.retriever.findall_part_for_root_probe('as_body')

                    root_probe_side = self.retriever.find_root_probe()

                    # keys
                    dict_key_list = []

                    for k in self.groupby_cols:
                        this_part_side = all_part_sides[0]
                        if k in this_part_side.columns:
                            joint_frame = this_part_side.get_retriever().find_merge(
                                mode='as_part').joint.get_joint_frame()
                            dict_key_list.append((k, joint_frame.part_lookup(k)))

                        if k in self.col_ins.keys():
                            v = self.col_ins[k]

                            if isinstance(v, (ColEl, ColExpr)):
                                col_name = v.field
                                for this_part_side in all_part_sides:
                                    if col_name in this_part_side.columns:
                                        joint_frame = this_part_side.get_retriever().find_merge(
                                            mode='as_part').joint.get_joint_frame()
                                        dict_key_list.append((k, joint_frame.part_lookup(col_name)))

                            if isinstance(v, ExternalExpr):
                                col_name = v.col.field
                                if col_name in root_probe_side.columns:
                                    dict_key_list.append((k, v.sdql_ir))

                    dict_key_ir = dict_key_list[0][1] if len(dict_key_list) == 1 else RecConsExpr(dict_key_list)

                    # vals
                    dict_val_list = []

                    for k in self.aggr_dict.keys():
                        v = self.aggr_dict[k]

                        if isinstance(v, RecAccessExpr):
                            col_name = v.name
                        else:
                            raise NotImplementedError

                        if col_name not in root_probe_side.columns:
                            if col_name in self.col_ins.keys():
                                col_op = self.col_ins[col_name].replace(rec=root_probe_side.iter_el.key)
                                dict_val_list.append((k, col_op))

                    dict_val_ir = RecConsExpr(dict_val_list)

                    # print({
                    #     'key': dict_key_ir,
                    #     'val': dict_val_ir
                    # })

                    joint_op = DicConsExpr([(dict_key_ir, dict_val_ir)])

                    if self.joint_cond:
                        cols_not_found = [x for x in self.retriever.find_cols(joint_cond)
                                          if x not in root_probe_side.columns]
                        if cols_not_found:
                            cond_replace_mapper = {}
                            # If found some columns are not in the root probe side
                            for c in cols_not_found:
                                for this_part_side in all_part_sides:
                                    if c in this_part_side.columns:
                                        this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                                        cond_replace_mapper[c] = DicLookupExpr(
                                            dicExpr=this_part_side.get_var_part(),
                                            keyExpr=root_probe_side.key_access(this_probe_key))

                            joint_op = IfExpr(condExpr=self.retriever.replace_cond(cond=self.joint_cond,
                                                                                   mapper=cond_replace_mapper).sdql_ir,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=EmptyDicConsExpr())
                        else:
                            joint_op = IfExpr(condExpr=self.joint_cond,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=EmptyDicConsExpr())

                    for this_part_side in all_part_sides:
                        this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                        joint_op = IfExpr(
                            condExpr=CompareExpr(CompareSymbol.NE,
                                                 DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                               keyExpr=root_probe_side.key_access(
                                                                   this_probe_key)),
                                                 ConstantExpr(None)),
                            thenBodyExpr=joint_op,
                            elseBodyExpr=EmptyDicConsExpr()
                        )

                    probe_side_conds = root_probe_side.get_retriever().findall_cond()
                    if probe_side_conds:
                        for probe_cond in probe_side_conds:
                            joint_op = IfExpr(condExpr=probe_cond,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=EmptyDicConsExpr())

                    joint_sum = SumExpr(varExpr=root_probe_side.iter_el.sdql_ir,
                                        dictExpr=root_probe_side.var_expr,
                                        bodyExpr=joint_op,
                                        isAssignmentSum=False)

                    vname_concat = f'x_{self.joint.name}'
                    var_concat = VarExpr(vname_concat)
                    self.joint.add_context_variable(vname_concat, var_concat)

                    flat_rec_list = []
                    if len(self.groupby_cols) == 1:
                        flat_rec_list.append((self.groupby_cols[0], PairAccessExpr(var_concat, 0)))
                    else:
                        for i in self.groupby_cols:
                            flat_rec_list.append((i, RecAccessExpr(PairAccessExpr(var_concat, 0), i)))
                    for j in self.aggr_dict.keys():
                        flat_rec_list.append((j, RecAccessExpr(PairAccessExpr(var_concat, 1), j)))

                    sum_concat = SumExpr(varExpr=var_concat,
                                         dictExpr=self.joint.var_expr,
                                         bodyExpr=DicConsExpr([(RecConsExpr(flat_rec_list),
                                                                ConstantExpr(True))]),
                                         isAssignmentSum=True)

                    var_out = VarExpr('out')
                    self.joint.add_context_variable('out', var_out)

                    out = LetExpr(var_out, sum_concat, ConstantExpr(True))

                    joint_let = LetExpr(varExpr=self.joint.var_expr,
                                        valExpr=joint_sum,
                                        bodyExpr=out)

                    return joint_let

                if self.part_frame.retriever.as_bypass_for_next_join:
                    if isinstance(self.probe_frame.probe_key, list):
                        prev_merge = self.probe_frame.probe_on.get_retriever().find_merge(mode='as_joint')

                        prev_part_side = prev_merge.left
                        prev_probe_side = prev_merge.right
                        prev_joint_frame = prev_merge.joint.get_joint_frame()
                        prev_part_frame = prev_merge.left.get_part_frame()
                        prev_probe_frame = prev_merge.right.get_probe_frame()

                        # keys
                        dict_key_list = []

                        for k in self.groupby_cols:
                            if k in prev_part_side.columns:
                                dict_key_list.append((k, prev_joint_frame.part_lookup(k)))

                        dict_key_ir = dict_key_list[0][1] if len(dict_key_list) == 1 else RecConsExpr(dict_key_list)

                        # vals
                        dict_val_list = []

                        for k in self.aggr_dict.keys():
                            v = self.aggr_dict[k]

                            if isinstance(v, RecAccessExpr):
                                col_name = v.name
                            else:
                                raise NotImplementedError

                            if col_name not in prev_probe_side.columns:
                                if col_name in self.col_ins.keys():
                                    col_op = self.col_ins[col_name].replace(rec=prev_probe_side.iter_el.key)
                                    dict_val_list.append((k, col_op))

                        dict_val_ir = RecConsExpr(dict_val_list)

                        # print({
                        #     'key': dict_key_ir,
                        #     'val': dict_val_ir
                        # })

                        joint_op = DicConsExpr([(dict_key_ir, dict_val_ir)])

                        lookup_keys = []

                        for i in self.probe_frame.probe_key:
                            if i in prev_probe_side.columns:
                                lookup_keys.append((i, prev_probe_side.key_access(i)))
                            if i in prev_part_side.columns:
                                lookup_keys.append((i, RecAccessExpr(DicLookupExpr(dicExpr=prev_part_frame.part_var,
                                                                                   keyExpr=prev_probe_side.key_access(
                                                                                       i)),
                                                                     i)))

                        joint_op = IfExpr(
                            condExpr=CompareExpr(CompareSymbol.NE,
                                                 DicLookupExpr(dicExpr=self.part_frame.part_var,
                                                               keyExpr=RecConsExpr(lookup_keys)),
                                                 ConstantExpr(None)),
                            thenBodyExpr=joint_op,
                            elseBodyExpr=EmptyDicConsExpr()
                        )

                        joint_op = IfExpr(
                            condExpr=CompareExpr(CompareSymbol.NE,
                                                 DicLookupExpr(dicExpr=prev_part_frame.part_var,
                                                               keyExpr=prev_probe_side.key_access(
                                                                   prev_probe_frame.probe_key)),
                                                 ConstantExpr(None)),
                            thenBodyExpr=joint_op,
                            elseBodyExpr=EmptyDicConsExpr()
                        )

                        if probe_cond:
                            joint_op = IfExpr(condExpr=probe_cond,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=EmptyDicConsExpr())

                        joint_sum = SumExpr(varExpr=prev_probe_side.iter_el.sdql_ir,
                                            dictExpr=prev_probe_side.var_expr,
                                            bodyExpr=joint_op,
                                            isAssignmentSum=False)

                        vname_concat = f'x_{self.joint.name}'
                        var_concat = VarExpr(vname_concat)
                        self.joint.add_context_variable(vname_concat, var_concat)

                        flat_rec_list = []
                        if len(self.groupby_cols) == 1:
                            flat_rec_list.append((self.groupby_cols[0], PairAccessExpr(var_concat, 0)))
                        else:
                            for i in self.groupby_cols:
                                flat_rec_list.append((i, RecAccessExpr(PairAccessExpr(var_concat, 0), i)))
                        for j in self.aggr_dict.keys():
                            flat_rec_list.append((j, RecAccessExpr(PairAccessExpr(var_concat, 1), j)))

                        sum_concat = SumExpr(varExpr=var_concat,
                                             dictExpr=probe_var,
                                             bodyExpr=DicConsExpr([(RecConsExpr(flat_rec_list),
                                                                    ConstantExpr(True))]),
                                             isAssignmentSum=True)

                        var_out = VarExpr('out')
                        self.joint.add_context_variable('out', var_out)

                        out = LetExpr(var_out, sum_concat, ConstantExpr(True))

                        joint_let = LetExpr(varExpr=probe_var,
                                            valExpr=joint_sum,
                                            bodyExpr=out)

                        return joint_let
                    else:
                        raise NotImplementedError

            if self.retriever.last_iter_is_merge:
                if isinstance(self.part_frame.get_part_key(), list) and \
                        isinstance(self.probe_frame.get_probe_key(), list):
                    raise NotImplementedError
                if isinstance(self.part_frame.get_part_key(), str) \
                        and isinstance(self.probe_frame.get_probe_key(), str):

                    # aggr_key_ir
                    key_rec_list = []
                    cleaned_col_proj = []
                    [cleaned_col_proj.append(x) for x in sorted(self.col_proj) if x not in cleaned_col_proj]
                    for col in cleaned_col_proj:
                        col_name = col[0]
                        if col_name in self.part_frame.cols_out:
                            key_rec_list.append((col_name,
                                                 RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                     keyExpr=probe_on.key_access(
                                                                                         probe_key)),
                                                               fieldName=col_name)))
                    aggr_key_ir = RecConsExpr(key_rec_list)

                    joint_op = IfExpr(
                        condExpr=CompareExpr(CompareSymbol.NE,
                                             DicLookupExpr(dicExpr=part_var,
                                                           keyExpr=probe_on.key_access(
                                                               probe_key)),
                                             ConstantExpr(None)),
                        thenBodyExpr=DicConsExpr([(aggr_key_ir, ConstantExpr(True))]),
                        elseBodyExpr=EmptyDicConsExpr()
                    )

                    if probe_cond:
                        joint_op = IfExpr(condExpr=probe_cond,
                                          thenBodyExpr=joint_op,
                                          elseBodyExpr=EmptyDicConsExpr())

                    joint_sum = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                        dictExpr=probe_on.var_expr,
                                        bodyExpr=joint_op,
                                        isAssignmentSum=False)

                    var_out = VarExpr('out')
                    self.joint.add_context_variable('out', var_out)

                    out = LetExpr(var_out, joint_sum, ConstantExpr(True))

                    return out
        else:
            if self.retriever.as_part_for_next_join:
                last_merge_expr = self.retriever.find_merge(mode='as_joint')
                next_merge_expr = self.retriever.find_merge(mode='as_part')

                # dict key: single column
                if isinstance(next_merge_expr.left_on, list):
                    raise NotImplementedError

                key_col = next_merge_expr.left_on

                dict_key_ir = self.probe_access(key_col)

                # dict vals: multi(single) column

                val_cols = [x for x in self.retriever.findall_cols_used()
                            if (x != last_merge_expr.left_on
                                and x != last_merge_expr.right_on
                                and x != key_col
                                and x not in self.retriever.find_renamed_cols())
                            or (x in self.retriever.find_cols_used('merge')
                                and x != key_col
                                and x not in self.retriever.find_renamed_cols())]

                dict_val_list = []

                for i in val_cols:
                    if i in self.part_frame.part_on.columns:
                        dict_val_list.append((i,
                                              RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                  keyExpr=probe_on.key_access(
                                                                                      probe_key)),
                                                            fieldName=i)))
                    if i in self.probe_frame.probe_on.columns:
                        dict_val_list.append((i,
                                              probe_on.key_access(i)))

                    if i in self.retriever.find_renamed_cols(mode='as_val'):
                        dict_val_list.append((i,
                                              probe_on.key_access(
                                                  self.retriever.find_col_rename(col_name=i,
                                                                                 by='val'))))

                dict_val_ir = RecConsExpr(dict_val_list)

                joint_op = DicConsExpr([(
                    dict_key_ir,
                    dict_val_ir
                )])

                transform = {
                    'to': self.joint.name,
                    'to_var': self.joint.var_expr,
                    'to_struct': OpRetType.DICT,
                    'struct_index': (tuple([key_col]), tuple(val_cols)),
                    'struct_types': None
                }

                self.probe_frame.probe_on.transform.migrate(transform)

                # print(self.joint.name)
                # print('all', self.joint.columns)
                # print('used', self.retriever.findall_cols_used())
                # print('last', last_merge_expr)
                # print('next', next_merge_expr)
                # print({
                #     'key': key_col,
                #     'vals': val_cols
                # })
                # print(transform)
                # print(joint_op)
                # print('==============================')

                if joint_cond:
                    joint_op = IfExpr(condExpr=joint_cond,
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=EmptyDicConsExpr())

                non_null_cond = CompareExpr(compareType=CompareSymbol.NE,
                                            leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                   keyExpr=probe_key_ir),
                                            rightExpr=ConstantExpr(None))

                joint_op = IfExpr(condExpr=non_null_cond,
                                  thenBodyExpr=joint_op,
                                  elseBodyExpr=EmptyDicConsExpr())

                if probe_cond:
                    joint_op = IfExpr(condExpr=probe_cond,
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=EmptyDicConsExpr())

                out = LetExpr(varExpr=probe_var,
                              valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                              dictExpr=probe_on.var_expr,
                                              bodyExpr=joint_op,
                                              isAssignmentSum=False),
                              bodyExpr=next_op)

                return out

            if self.retriever.as_probe_for_next_join:
                next_merge = self.retriever.find_merge('as_probe')
                if next_merge:
                    if next_merge.left.get_retriever().as_bypass_for_next_join:
                        return next_op

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
            dict_val_list = []
            if self.col_ins:
                for k in self.aggr_dict.keys():
                    v = self.aggr_dict[k]
                    if v.name in self.col_ins.keys():
                        col_expr = self.col_ins[v.name].sdql_ir
                    else:
                        col_expr = v
                    dict_val_list.append((k, col_expr))
            else:
                for k in self.aggr_dict.keys():
                    dict_val_list.append((k, self.aggr_dict[k]))
            aggr_val_ir = RecConsExpr(dict_val_list)

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

            out = LetExpr(varExpr=probe_var,
                          valExpr=sum_expr,
                          bodyExpr=LetExpr(var_out, sum_concat, ConstantExpr(True)))

            return out
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
                this_part_side = self.partition_frame.partition_on
                next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                     on=this_part_side)

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

            out = LetExpr(varExpr=probe_var,
                          valExpr=sum_expr,
                          bodyExpr=LetExpr(var_out, sum_concat, ConstantExpr(True)))

            return out
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

                dict_val_list = []

                if this_part_col_proj:
                    for col in this_part_col_proj:
                        field = col[0]
                        if field != next_probe_key:
                            if field in this_part_cols:
                                dict_val_list.append((field, RecAccessExpr(recExpr=DicLookupExpr(dicExpr=this_part_var,
                                                                                                 keyExpr=this_probe_key_ir),
                                                                           fieldName=field)))
                if next_part_col_proj:
                    for col in next_part_col_proj:
                        field = col[0]
                        if field != next_part_key:
                            if next_probe_key not in probe_cols:
                                dict_val_list.append((field,
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

                iter_val_ir = RecConsExpr(dict_val_list)

                if joint_cond:
                    this_part_side = self.partition_frame.partition_on
                    next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                         on=this_part_side).sdql_ir

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

                out = LetExpr(varExpr=probe_var,
                              valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                              dictExpr=probe_on.var_expr,
                                              bodyExpr=joint_op,
                                              isAssignmentSum=False),
                              bodyExpr=next_op)

                return out
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
                    this_part_side = self.partition_frame.partition_on
                    next_joint_cond = joint_cond.replace(rec=DicLookupExpr(part_var, probe_key_ir),
                                                         on=this_part_side).sdql_ir

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

                isin_expr = self.probe_frame.find_isin()
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

                out = LetExpr(varExpr=probe_var,
                              valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                              dictExpr=probe_on.var_expr,
                                              bodyExpr=joint_op,
                                              isAssignmentSum=True),
                              bodyExpr=next_op)

                return out

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
                    last_op = IfExpr(last_cond.replace(PairAccessExpr(x_var_part, 1)),
                                     last_op,
                                     EmptyDicConsExpr())

                sum_expr = SumExpr(varExpr=x_var_part,
                                   dictExpr=var_part,
                                   bodyExpr=last_op,
                                   isAssignmentSum=True)

                var_out = VarExpr('out')
                self.joint.add_context_variable('out', var_out)

                out = LetExpr(varExpr=var_out,
                              valExpr=sum_expr,
                              bodyExpr=ConstantExpr(True))

                return out

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
                dict_val_list = []
                if self.col_ins:
                    for k in self.aggr_dict.keys():
                        v = self.aggr_dict[k]
                        if v.name in self.col_ins.keys():
                            col_expr = self.col_ins[v.name].sdql_ir
                        else:
                            col_expr = v
                        dict_val_list.append((k, col_expr))
                else:
                    for k in self.aggr_dict.keys():
                        dict_val_list.append((k, self.aggr_dict[k]))
                aggr_val_ir = RecConsExpr(dict_val_list)

                joint_groupby_aggr_op = DicConsExpr([(aggr_key_ir,
                                                      aggr_val_ir
                                                      )])

                if joint_cond:
                    joint_groupby_aggr_op = IfExpr(condExpr=joint_cond,
                                                   thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                              aggr_val_ir)]),
                                                   elseBodyExpr=EmptyDicConsExpr())

                isin_expr = self.probe_frame.find_isin()
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

                out = LetExpr(varExpr=probe_var,
                              valExpr=sum_expr,
                              bodyExpr=LetExpr(var_out, sum_concat, ConstantExpr(True)))

                return out
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

                out = LetExpr(varExpr=probe_var,
                              valExpr=sum_expr,
                              bodyExpr=LetExpr(var_out, calc_expr, ConstantExpr(True)))

                return out

                # print(self.col_ins)
                # print(self.probe_frame.get_probe_cond())
                # print(self.get_next_calc().sdql_ir)

            # print('nothing')

    def get_joint_expr(self, next_op=None):
        if not next_op:
            if self.is_groupby_agg_joint:
                next_op = self.get_probe_expr()
            elif self.retriever.is_last_joint():
                next_op = self.get_probe_expr()
            else:
                next_op = ConstantExpr('placeholder_probe_next')

        # print(self.partition_frame.is_joint, self.probe_frame.is_joint)
        # print(self.part_frame.partition_on.name, self.probe_frame.probe_on.name)

        # Q15 -> this way, sir
        # Q16 -> this way, sir
        # Q19 -> this way, sir
        if not self.part_frame.is_joint and not self.probe_frame.is_joint:
            if self.probe_frame.was_isin:
                return self.probe_frame.probe_on.get_having(self.part_frame.get_part_expr(self.get_probe_expr(next_op)))
            if self.probe_frame.was_groupby_agg:
                tmp_let_expr = self.probe_frame.probe_on.get_groupby_agg()
                tmp_let_expr = LetExpr(self.probe_frame.get_probe_on().get_var_part(),
                                       tmp_let_expr.valExpr,
                                       self.part_frame.get_part_expr(self.get_probe_expr(next_op)))
                return tmp_let_expr
            # print(f'{self.joint.name}: neither joint')
            return self.part_frame.get_part_expr(self.get_probe_expr(next_op))
        if self.part_frame.is_joint and not self.probe_frame.is_joint:
            # print(f'{self.joint.name}: part joint')
            # return self.part_frame.part_on.get_joint_frame().get_joint_expr(next_op)
            return self.part_frame.part_on.get_joint_frame().get_joint_expr(self.get_probe_expr(next_op))
        if not self.part_frame.is_joint and self.probe_frame.is_joint:
            # print(f'{self.joint.name}: probe joint')
            if self.part_frame.retriever.as_part_for_next_join:
                # print(f'completely bypass\n'
                #       f'joint: {self.joint.name}\n'
                #       f'part: {self.part_frame.part_on.name}\n'
                #       f'probe: {self.probe_frame.probe_on.name}')
                return self.probe_frame.probe_on.get_retriever().find_merge(
                    mode='as_joint').joint.get_joint_frame().get_joint_expr(
                    self.part_frame.get_part_expr(self.get_probe_expr(next_op)))
            return self.part_frame.get_part_expr(self.probe_frame.probe_on.get_joint_frame().get_joint_expr(next_op))
        if self.part_frame.is_joint and self.probe_frame.is_joint:
            if self.retriever.find_illegal_dup_col():
                raise ValueError(f'Detected duplicated columns in merge: {self.retriever.find_illegal_dup_col()}')

            '''
            For any joint:
                If probe side is joint:
                    Then find the probe side of the joint probe side.
                    Until the probe side is not joint.
                If the probe side is not joint:
                    Then start with this dataframe
                    Recursively add dictionary lookup conditions
                    
            Need a function to find the root probe side (the one not joint).
                retriever.find_root_probe_side()
                
            Need a function to find all partition side on the path of the root probe side.
                (If the root probe side occurs as the probe side in a join)
                retriever.findall_part_for_root()
                
            As the end of query, we now have following information:
                1. groupby aggregation occurs
                2. probe side is joint
                
            Therefore, we must reconstruct the process based on __this__ (end):
                Start with: Groupby Aggregation
                End with: First merge for the root probe side
                Preserved:
                    All partitions
                Skipped:
                    Formation of joint dataframes 
                    (any intermediate joint except the one contains the root probe side)
            
            '''

            all_bindings = []
            all_bindings += self.retriever.findall_part_for_root_probe('as_expr')
            all_bindings.append(self.get_probe_expr(next_op))

            # for i in all_bindings:
            #     print(i)

            return SDQLInspector.concat_bindings(all_bindings)

        raise NotImplementedError

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
        if not self.groupby_cols and self.aggr_dict:
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

    def get_next_part_frame(self) -> JoinPartFrame:
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
