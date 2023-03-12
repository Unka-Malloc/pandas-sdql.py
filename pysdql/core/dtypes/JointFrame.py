from pysdql.core.dtypes import ColOpExpr, ColEl, ColExtExpr, GroupbyAggrExpr, AggrExpr, NewColOpExpr, OldColOpExpr
from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import OpRetType, AggrType
from pysdql.core.dtypes.JoinPartFrame import JoinPartFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.SDQLInspector import SDQLInspector

from pysdql.core.dtypes.sdql_ir import *
from pysdql.core.util.df_retriever import Retriever
from pysdql.extlib.sdqlpy.sdql_lib import sr_dict


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

    def part_lookup(self, col_name=''):
        if col_name:
            if isinstance(self.probe_frame.probe_key, str):
                return RecAccessExpr(recExpr=DicLookupExpr(dicExpr=self.part_frame.part_var,
                                                           keyExpr=self.probe_frame.probe_on.key_access(
                                                               self.probe_frame.probe_key)),
                                     fieldName=col_name)
            elif isinstance(self.probe_frame.probe_key, list):
                return RecAccessExpr(recExpr=DicLookupExpr(dicExpr=self.part_frame.part_var,
                                                           keyExpr=RecConsExpr([(c,
                                                                                 self.probe_access(c))
                                                                                for c in self.probe_frame.probe_key])),
                                     fieldName=col_name)
            else:
                raise ValueError()
        else:
            if isinstance(self.probe_frame.probe_key, str):
                return DicLookupExpr(
                    dicExpr=self.part_frame.part_var,
                    keyExpr=self.probe_frame.probe_on.key_access(
                        self.probe_frame.probe_key))
            elif isinstance(self.probe_frame.probe_key, list):
                return DicLookupExpr(
                    dicExpr=self.part_frame.part_var,
                    keyExpr=RecConsExpr([(c,
                                          self.probe_frame.probe_on.key_access(c))
                                         for c in self.probe_frame.probe_key]))

    def part_nonull(self):
        # print(self.part_frame.get_part_dict_key())
        # print(self.part_frame.get_part_dict_val())

        if isinstance(self.probe_frame.probe_key, list):
            return CompareExpr(CompareSymbol.NE,
                               DicLookupExpr(dicExpr=self.part_frame.part_var,
                                             keyExpr=RecConsExpr([(c,
                                                                   self.probe_frame.probe_on.key_access(c))
                                                                  for c in self.probe_frame.probe_key])),
                               ConstantExpr(None))
        else:
            return CompareExpr(CompareSymbol.NE,
                               DicLookupExpr(dicExpr=self.part_frame.part_var,
                                             keyExpr=self.probe_frame.probe_on.key_access(
                                                 self.probe_frame.probe_key)),
                               ConstantExpr(None))

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

        # df: partition side
        part_on = self.part_frame.part_on
        # var:
        part_var = self.part_frame.part_var
        this_part_col_proj = self.part_frame.get_part_col_proj()

        # str: left_on
        part_key = self.part_frame.part_key

        # df: probe side
        probe_on = self.probe_frame.get_probe_on()
        # var: joint dict
        probe_var = self.joint.var_expr

        # str: right_on
        probe_key = self.probe_frame.get_probe_key()
        probe_key_ir = probe_on.key_access(probe_key)

        probe_cond = self.probe_frame.retriever.find_cond()
        this_probe_col_proj = self.probe_frame.get_probe_col_proj()

        joint_cond = self.joint_cond
        joint_col_proj = self.retriever.find_col_proj()

        if self.retriever.is_last_joint():
            if self.retriever.last_iter_is_aggr:
                # Q19

                aggr_info = self.retriever.find_aggr()

                aggr_dict = aggr_info.aggr_op

                cond_mapper = {tuple(part_on.columns): DicLookupExpr(part_var, probe_key_ir),
                               tuple(probe_on.columns): probe_on.iter_el.key}

                joint_cond = joint_cond.replace(rec=None,
                                                inplace=False,
                                                mapper=cond_mapper)

                col_ins = self.retriever.find_col_ins_before(AggrExpr)

                if len(aggr_dict.keys()) == 1:
                    '''
                    Aggregation as a single value
                    Then format to a singleton dictionary
                    '''
                    dict_val = list(aggr_dict.items())[0][1]

                    if isinstance(dict_val, RecAccessExpr):
                        # (, 'sum')
                        dict_val_name = dict_val.name

                        if dict_val_name in probe_on.columns:
                            aggr_body = probe_on.key_access(dict_val_name)
                        else:
                            if dict_val_name in col_ins.keys():
                                aggr_body = col_ins[dict_val_name].replace(probe_on.iter_el.key)
                            else:
                                raise IndexError(f'Cannot find column {dict_val_name} in {probe_on.columns}')

                        if joint_cond:
                            aggr_body = IfExpr(condExpr=joint_cond,
                                               thenBodyExpr=aggr_body,
                                               elseBodyExpr=ConstantExpr(0.0))
                    elif isinstance(dict_val, ConstantExpr):
                        # (, 'count')
                        aggr_body = dict_val

                        if joint_cond:
                            aggr_body = IfExpr(condExpr=joint_cond,
                                               thenBodyExpr=aggr_body,
                                               elseBodyExpr=ConstantExpr(0))
                    else:
                        raise NotImplementedError
                else:
                    '''
                    Aggregation as a single record
                    Then format to a singleton dictionary
                    '''
                    aggr_tuples = []
                    for k in aggr_dict.keys():
                        v = aggr_dict[k]
                        if isinstance(v, RecAccessExpr):
                            # (, 'sum')
                            v_name = v.name
                            if v_name in probe_on.columns:
                                aggr_tuples.append((k, probe_on.key_access(v_name)))
                            else:
                                if v_name in col_ins.keys():
                                    aggr_tuples.append((k,
                                                        col_ins[v_name].replace(probe_on.iter_el.key)))
                                else:
                                    raise IndexError(f'Cannot find column {k} in {probe_on.columns}')
                        elif isinstance(v, ConstantExpr):
                            # (, 'count')
                            aggr_tuples.append((k, v))
                        else:
                            raise NotImplementedError

                    aggr_body = RecConsExpr(aggr_tuples)

                    if joint_cond:
                        aggr_body = IfExpr(condExpr=joint_cond,
                                           thenBodyExpr=aggr_body,
                                           elseBodyExpr=ConstantExpr(None))

                aggr_body = SDQLInspector.add_cond(sdql_obj=aggr_body,
                                                   cond=self.part_nonull(),
                                                   layer='outer')

                if probe_cond:
                    aggr_body = SDQLInspector.add_cond(sdql_obj=aggr_body,
                                                       cond=probe_cond.sdql_ir,
                                                       layer='outer')

                aggr_sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                        dictExpr=probe_on.var_expr,
                                        bodyExpr=aggr_body,
                                        isAssignmentSum=False)

                vname_aggr = f'{probe_on.name}_aggr'
                var_aggr = VarExpr(vname_aggr)
                probe_on.add_context_variable(vname_aggr,
                                              var_aggr)

                aggr_let_expr = LetExpr(varExpr=var_aggr,
                                        valExpr=aggr_sum_expr,
                                        bodyExpr=ConstantExpr(True))

                if len(aggr_dict.keys()) == 1:
                    dict_key = list(aggr_dict.items())[0][0]

                    if aggr_info.aggr_type == AggrType.Dict:
                        format_op = DicConsExpr([(RecConsExpr([(dict_key, var_aggr)]), ConstantExpr(True))])
                    elif aggr_info.aggr_type == AggrType.Scalar:
                        format_op = var_aggr
                    else:
                        raise NotImplementedError
                else:
                    format_op = DicConsExpr([(var_aggr, ConstantExpr(True))])

                vname_res = f'results'
                var_res = VarExpr(vname_res)
                probe_on.add_context_variable(vname_res,
                                              var_res)

                form_let_expr = LetExpr(varExpr=var_res,
                                        valExpr=format_op,
                                        bodyExpr=ConstantExpr(True))

                return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])
            # Q5
            # Q7
            # Q8
            if self.retriever.last_iter_is_groupby_aggr:
                # Q11
                if self.retriever.was_filter:
                    aggr_filt = self.retriever.find_aggr_filt()

                    groupby_aggr_info = self.retriever.find_groupby_aggr()

                    aggr_dict = groupby_aggr_info.aggr_dict
                    groupby_cols = groupby_aggr_info.groupby_cols
                    joint_col_ins = self.retriever.find_col_ins_before(GroupbyAggrExpr)

                    if len(groupby_cols) == 0:
                        raise ValueError()
                    elif len(groupby_cols) == 1:
                        dict_key_ir = probe_on.key_access(groupby_cols[0])
                    else:
                        key_tuples = []

                        for c in groupby_cols:
                            if c == probe_key:
                                key_tuples.append((c, probe_key_ir))
                            elif c == part_key:
                                key_tuples.append((c, probe_key_ir))
                            elif c in probe_on.columns:
                                key_tuples.append((c, self.probe_access(c)))
                            elif c in part_on.columns:
                                key_tuples.append((c, self.part_lookup(c)))
                            else:
                                raise IndexError(f'Cannot find such a column {c} '
                                                 f'in part side {part_on.name} '
                                                 f'and probe side {probe_on.name}')

                        dict_key_ir = RecConsExpr(key_tuples)

                    if len(aggr_dict.keys()) == 0:
                        raise ValueError()
                    elif len(aggr_dict.keys()) == 1:
                        '''
                        Aggregation as a single value
                        Then format to a singleton dictionary
                        '''

                        dict_val = list(aggr_dict.items())[0][1]

                        if isinstance(dict_val, RecAccessExpr):
                            # (, 'sum')
                            dict_val_name = dict_val.name

                            if dict_val_name in probe_on.columns:
                                aggr_body = DicConsExpr([(dict_key_ir,
                                                          probe_on.key_access(dict_val_name))])
                            else:
                                if dict_val_name in joint_col_ins.keys():
                                    aggr_body = DicConsExpr([(dict_key_ir,
                                                              joint_col_ins[dict_val_name].replace(
                                                                  probe_on.iter_el.key))])
                                else:
                                    raise IndexError(
                                        f'Cannot find column {dict_val_name} in {probe_on.columns}')
                        elif isinstance(dict_val, ConstantExpr):
                            # (, 'count')
                            aggr_body = DicConsExpr([(dict_key_ir,
                                                      dict_val)])
                        else:
                            raise NotImplementedError
                    else:
                        '''
                        Aggregation as a single record
                        Then format to a singleton dictionary
                        '''
                        val_tuples = []
                        for k in aggr_dict.keys():
                            v = aggr_dict[k]

                            if isinstance(v, RecAccessExpr):
                                # (, 'sum')
                                v_name = v.name

                                if v_name in probe_on.columns:
                                    val_tuples.append((k, probe_on.key_access(v_name)))
                                else:
                                    if v_name in joint_col_ins.keys():
                                        val_tuples.append((k,
                                                           joint_col_ins[v_name].replace(probe_on.iter_el.key)))
                                    else:
                                        raise IndexError(f'Cannot find column {v_name} in {probe_on.columns}')
                            elif isinstance(v, ConstantExpr):
                                # (, 'count')
                                val_tuples.append((k, v))
                            else:
                                raise NotImplementedError

                        aggr_body = DicConsExpr([(dict_key_ir, RecConsExpr(val_tuples))])

                    aggr_body = sr_dict(dict(aggr_body.initialPairs))

                    filt_list = [('filt_val', aggr_filt.aggr_unit2.sdql_ir), ('filt_agg', aggr_body)]

                    filt_body = RecConsExpr(filt_list)

                    filt_body = IfExpr(self.part_nonull(),
                                       filt_body,
                                       ConstantExpr(None))

                    aggr_sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                            dictExpr=probe_on.var_expr,
                                            bodyExpr=filt_body,
                                            isAssignmentSum=False)

                    vname_aggr = f'{probe_on.name}_aggr'
                    var_aggr = VarExpr(vname_aggr)
                    probe_on.add_context_variable(vname_aggr,
                                                  var_aggr)

                    aggr_let_expr = LetExpr(varExpr=var_aggr,
                                            valExpr=aggr_sum_expr,
                                            bodyExpr=ConstantExpr(True))

                    vname_x_aggr = f'x_{vname_aggr}'
                    var_x_aggr = VarExpr(vname_x_aggr)
                    probe_on.add_context_variable(vname_x_aggr,
                                                  var_x_aggr)

                    # aggr = {? : scalar}
                    if len(aggr_dict.keys()) == 1:
                        dict_key = list(aggr_dict.items())[0][0]

                        format_key_tuples = []

                        # aggr = {scalar : scalar}
                        if len(groupby_cols) == 1:
                            format_key_tuples.append((groupby_cols[0],
                                                      PairAccessExpr(var_x_aggr, 0)))
                        # aggr = {record : scalar}
                        else:
                            for c in groupby_cols:
                                format_key_tuples.append((c, RecAccessExpr(PairAccessExpr(var_x_aggr, 0), c)))

                        format_key_tuples.append((dict_key, PairAccessExpr(var_x_aggr, 1)))

                        format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                                  ConstantExpr(True))])
                    # aggr = {? : record}
                    else:
                        # aggr = {scalar: record}
                        if len(groupby_cols) == 1:
                            format_key_tuples = [(groupby_cols[0],
                                                  PairAccessExpr(var_x_aggr, 0))]

                            for k in aggr_dict.keys():
                                format_key_tuples.append((k,
                                                          RecAccessExpr(PairAccessExpr(var_x_aggr,
                                                                                       1),
                                                                        k)))

                            format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                                      ConstantExpr(True))])
                        else:
                            # aggr = {record : record}
                            format_op = DicConsExpr([(ConcatExpr(PairAccessExpr(var_x_aggr, 0),
                                                                 PairAccessExpr(var_x_aggr, 1)),
                                                      ConstantExpr(True))])

                    format_op = IfExpr(CompareExpr(aggr_filt.cond_op,
                                                   PairAccessExpr(var_x_aggr, 1),
                                                   RecAccessExpr(var_aggr, 'filt_val')),
                                       format_op,
                                       ConstantExpr(None))

                    format_sum = SumExpr(varExpr=var_x_aggr,
                                         dictExpr=RecAccessExpr(var_aggr, 'filt_agg'),
                                         bodyExpr=format_op,
                                         isAssignmentSum=True)

                    vname_res = f'results'
                    var_res = VarExpr(vname_res)
                    probe_on.add_context_variable(vname_res,
                                                  var_res)

                    form_let_expr = LetExpr(varExpr=var_res,
                                            valExpr=format_sum,
                                            bodyExpr=ConstantExpr(True))

                    return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])

                if self.probe_frame.retriever.is_joint:
                    # Q5
                    if self.part_frame.retriever.as_bypass_for_next_join:
                        '''
                        Hello from Yizhuo,
                        This comment is in chinese, it indicates the assumption for this optimization
                        本优化预设了
                        1. 上一个融合的probe side必定是root, 这时我们只需要预设当前的融合不存在, 并且向上一个融合回退一步就行了
                        2. 当前的融合存在列表作为融合的键, 也就是说, 必须存在 left_on=[] and right_on=[]
                        3. 当前的bypass part side确实进行了一次遍历, 
                            而唯一的原因是当前的merge需要这次遍历, 这也就是它叫做bypass的原因
                        4. 如果上一个融合的probe side依然是joint之后的结果
                            它也许会从get_joint_expr()开始
                            走向 part_is_joint and probe_is_joint 这个分支
                            也有可能走向其它的分支, 这取决于另外一个part side的状态
                        
                        不论如何, 它们都要求之前必须存在多个part side.
                        
                        如果未来需要将这个branch进行优化
                        那么:
                            1. 推导出基于 str 的 merge
                            2. 将上一级probe side的 merge 更改为 root probe side 的 merge, 
                                这将会需要检查路径上的所有bypass part side, 并且为其生成相应的expr
                                并且将当前需要用到的列在所有part side上搜索.
                        优化后的branch将会更加通用 
                        '''

                        # print(self.part_frame.part_on.name, 'is bypass')
                        if isinstance(self.part_frame.part_key, list) \
                                and isinstance(self.probe_frame.probe_key, list):
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
                                    col_op = v.name
                                else:
                                    raise NotImplementedError

                                if col_op not in prev_probe_side.columns:
                                    if col_op in self.col_ins.keys():
                                        new_col = self.col_ins[col_op].replace(rec=prev_probe_side.iter_el.key)
                                        dict_val_list.append((k, new_col))

                            dict_val_ir = RecConsExpr(dict_val_list)

                            joint_op = DicConsExpr([(dict_key_ir, dict_val_ir)])

                            lookup_keys = []

                            for i in self.probe_frame.probe_key:
                                if i in prev_part_side.columns:
                                    lookup_keys.append((i, RecAccessExpr(DicLookupExpr(dicExpr=prev_part_frame.part_var,
                                                                                       keyExpr=prev_probe_side.key_access(
                                                                                           prev_probe_frame.probe_key)),
                                                                         i)))
                                elif i in prev_probe_side.columns:
                                    lookup_keys.append((i, prev_probe_side.key_access(i)))

                            joint_op = IfExpr(
                                condExpr=CompareExpr(CompareSymbol.NE,
                                                     DicLookupExpr(dicExpr=self.part_frame.part_var,
                                                                   keyExpr=RecConsExpr(lookup_keys)),
                                                     ConstantExpr(None)),
                                thenBodyExpr=joint_op,
                                elseBodyExpr=ConstantExpr(None)
                            )

                            joint_op = IfExpr(
                                condExpr=CompareExpr(CompareSymbol.NE,
                                                     DicLookupExpr(dicExpr=prev_part_frame.part_var,
                                                                   keyExpr=prev_probe_side.key_access(
                                                                       prev_probe_frame.probe_key)),
                                                     ConstantExpr(None)),
                                thenBodyExpr=joint_op,
                                elseBodyExpr=ConstantExpr(None)
                            )

                            if probe_cond:
                                joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))

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

                            var_res = VarExpr('results')
                            self.joint.add_context_variable('results', var_res)

                            out = LetExpr(var_res, sum_concat, ConstantExpr(True))

                            joint_let = LetExpr(varExpr=probe_var,
                                                valExpr=joint_sum,
                                                bodyExpr=out)

                            return joint_let

                    else:
                        # Q7
                        # Q8
                        # Q9
                        # Q21

                        # If the probe side is joint, then there must be multiple partitions
                        # Therefore, we need to probe on every partition

                        # print(f'part side: {self.part_frame.part_on} is not bypass')
                        # print(f'{self.part_frame.retriever.findall_cols_used(only_next=True)} are used in the future')

                        additional_non_null = []

                        all_part_sides = self.retriever.findall_part_for_root_probe('as_body')

                        root_probe_side = self.retriever.find_root_probe()
                        root_part_side = self.retriever.find_root_part()

                        # keys
                        dict_key_list = []

                        for k in self.groupby_cols:
                            k_found = False

                            if k in root_part_side.columns:
                                dict_key_list.append((k, root_probe_side.key_access(k)))
                                k_found = True
                            elif k in self.col_ins.keys():
                                v = self.col_ins[k]
                                if isinstance(v, (ColEl, ColOpExpr)):
                                    col_op = v.field
                                    for this_part_side in all_part_sides:
                                        if col_op in this_part_side.columns:
                                            joint_frame = this_part_side.get_retriever().find_merge(
                                                mode='as_part').joint.get_joint_frame()
                                            dict_key_list.append((k, joint_frame.part_lookup(col_op)))
                                            k_found = True
                                elif isinstance(v, ColExtExpr):
                                    col_op = v.col.field
                                    if col_op in root_probe_side.columns:
                                        dict_key_list.append((k, v.sdql_ir))
                                        k_found = True
                                    else:
                                        for this_part_side in all_part_sides:
                                            if col_op in this_part_side.columns:
                                                joint_frame = this_part_side.get_retriever().find_merge(
                                                    mode='as_part').joint.get_joint_frame()
                                                dict_key_list.append((k,
                                                                      v.replace(rec=joint_frame.part_lookup(col_op),
                                                                                inplace=True).sdql_ir))
                                                k_found = True
                                else:
                                    raise ValueError(f'Unsupported type {type(v)}')
                            elif k in root_probe_side.columns:
                                dict_key_list.append((k, root_probe_side.key_access(k)))
                                k_found = True
                            else:
                                for this_part_side in all_part_sides:
                                    if k in this_part_side.columns:
                                        dict_key_list.append((k, self.retriever.find_lookup_path(self, k)))
                                        k_found = True

                            if not k_found:
                                raise IndexError(f'Not found {k}')

                        dict_key_ir = dict_key_list[0][1] if len(dict_key_list) == 1 else RecConsExpr(dict_key_list)

                        # vals
                        dict_val_list = []

                        for k in self.aggr_dict.keys():
                            v = self.aggr_dict[k]

                            if isinstance(v, RecAccessExpr):
                                col_op = v.name
                                if col_op in root_probe_side.columns:
                                    dict_val_list.append((k, root_probe_side.key_access(k)))
                                # For columns insert
                                elif col_op in self.col_ins.keys():
                                    new_col = self.col_ins[col_op]
                                    if isinstance(new_col, IfExpr):
                                        dict_val_list.append((k, new_col))
                                    else:
                                        col_mapper = {}

                                        col_mapper[tuple(root_probe_side.columns)] = root_probe_side.iter_el.key

                                        for this_part_side in all_part_sides:
                                            this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                                            if isinstance(this_probe_key, list):
                                                lookup_expr = DicLookupExpr(
                                                    dicExpr=this_part_side.get_var_part(),
                                                    keyExpr=RecConsExpr([(c,
                                                                          root_probe_side.key_access(c))
                                                                         for c in self.probe_frame.probe_key]))

                                                col_mapper[tuple(this_part_side.cols_out)] = lookup_expr
                                            else:
                                                lookup_expr = DicLookupExpr(
                                                    dicExpr=this_part_side.get_var_part(),
                                                    keyExpr=self.retriever.find_lookup_path(self, this_probe_key))

                                                col_mapper[tuple(this_part_side.cols_out)] = lookup_expr

                                            additional_non_null.append(lookup_expr)

                                        dict_val_list.append(
                                            (k, new_col.replace(rec=None, inplace=False, mapper=col_mapper)))
                            elif isinstance(v, ConstantExpr):
                                dict_val_list.append((k, v))
                            else:
                                raise NotImplementedError

                        dict_val_ir = RecConsExpr(dict_val_list) if dict_val_list else ConstantExpr(True)

                        # print(
                        #     'key', dict_key_ir,
                        #     '\nval', dict_val_ir
                        # )

                        joint_op = DicConsExpr([(dict_key_ir, dict_val_ir)])

                        for non_null_cond in additional_non_null:
                            joint_op = IfExpr(condExpr=non_null_cond,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                        if joint_cond:
                            cols_not_in_root_probe = [x for x in self.retriever.find_cols(joint_cond)
                                                      if x not in root_probe_side.columns]

                            if cols_not_in_root_probe:
                                cond_replace_mapper = {}
                                # If found some columns are not in the root probe side
                                for c in cols_not_in_root_probe:
                                    for this_part_side in all_part_sides:
                                        if c in this_part_side.cols_out:
                                            this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                                            cond_replace_mapper[c] = RecAccessExpr(
                                                recExpr=DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                      keyExpr=root_probe_side.key_access(
                                                                          this_probe_key)),
                                                fieldName=c
                                            )

                                joint_op = IfExpr(condExpr=self.retriever.replace_cond(cond=joint_cond,
                                                                                       mapper=cond_replace_mapper).sdql_ir,
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))
                            else:
                                joint_op = IfExpr(condExpr=joint_cond,
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))

                        for this_part_side in all_part_sides:
                            this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                            if this_probe_key in root_probe_side.columns:
                                joint_op = IfExpr(
                                    condExpr=CompareExpr(CompareSymbol.NE,
                                                         DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                       keyExpr=root_probe_side.key_access(
                                                                           this_probe_key)),
                                                         ConstantExpr(None)),
                                    thenBodyExpr=joint_op,
                                    elseBodyExpr=ConstantExpr(None)
                                )
                            else:
                                if isinstance(this_probe_key, list):
                                    lookup_list = []
                                    for k in this_probe_key:
                                        lookup_key = self.retriever.find_lookup_path(self, k)
                                        lookup_list.append((k, lookup_key))

                                    joint_op = IfExpr(
                                        condExpr=CompareExpr(CompareSymbol.NE,
                                                             DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                           keyExpr=RecConsExpr(lookup_list)),
                                                             ConstantExpr(None)),
                                        thenBodyExpr=joint_op,
                                        elseBodyExpr=ConstantExpr(None)
                                    )
                                else:
                                    lookup_key = self.retriever.find_lookup_path(self, this_probe_key)
                                    joint_op = IfExpr(
                                        condExpr=CompareExpr(CompareSymbol.NE,
                                                             DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                           keyExpr=lookup_key),
                                                             ConstantExpr(None)),
                                        thenBodyExpr=joint_op,
                                        elseBodyExpr=ConstantExpr(None)
                                    )

                        probe_side_conds = root_probe_side.get_retriever().findall_cond()
                        if probe_side_conds:
                            for probe_cond in probe_side_conds:
                                joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))

                        joint_sum = SumExpr(varExpr=root_probe_side.iter_el.sdql_ir,
                                            dictExpr=root_probe_side.var_expr,
                                            bodyExpr=joint_op,
                                            isAssignmentSum=False)

                        vname_concat = f'x_{self.joint.name}'
                        var_concat = VarExpr(vname_concat)
                        self.joint.add_context_variable(vname_concat, var_concat)

                        col_proj_after_aggr = self.retriever.find_col_proj_after(GroupbyAggrExpr)
                        col_ins_after_aggr = self.retriever.find_col_ins_after(GroupbyAggrExpr)

                        flat_rec_list = []

                        if col_proj_after_aggr:
                            for c in col_proj_after_aggr.proj_cols:
                                if c in self.groupby_cols:
                                    if len(self.groupby_cols) == 1:
                                        flat_rec_list.append((c, PairAccessExpr(var_concat, 0)))
                                    else:
                                        flat_rec_list.append((c, RecAccessExpr(PairAccessExpr(var_concat, 0), c)))
                                elif c in self.aggr_dict.keys():
                                    flat_rec_list.append((c, RecAccessExpr(PairAccessExpr(var_concat, 1), c)))
                                elif c in col_ins_after_aggr.keys():
                                    flat_rec_list.append(
                                        (c, col_ins_after_aggr[c].replace(PairAccessExpr(var_concat, 1))))
                        else:
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

                        if next_op:
                            joint_let = LetExpr(varExpr=self.joint.var_expr,
                                                valExpr=joint_sum,
                                                bodyExpr=next_op)
                            return joint_let
                        else:
                            var_res = VarExpr('results')
                            self.joint.add_context_variable('results', var_res)

                            out = LetExpr(var_res, sum_concat, ConstantExpr(True))

                            joint_let = LetExpr(varExpr=self.joint.var_expr,
                                                valExpr=joint_sum,
                                                bodyExpr=out)
                            return joint_let
                else:
                    # Q3
                    # Q16
                    # Q18
                    if self.part_frame.retriever.was_probed:

                        groupby_aggr_info = self.retriever.find_groupby_aggr()

                        aggr_dict = groupby_aggr_info.aggr_dict
                        groupby_cols = groupby_aggr_info.groupby_cols

                        joint_cond = self.retriever.find_cond_before(GroupbyAggrExpr)
                        joint_col_ins = self.retriever.find_col_ins_before(GroupbyAggrExpr)
                        probe_isin_expr = self.probe_frame.retriever.find_isin_before(MergeExpr)

                        if len(groupby_cols) == 0:
                            raise ValueError()
                        elif len(groupby_cols) == 1:
                            dict_key_ir = probe_on.key_access(groupby_cols[0])
                        else:
                            key_tuples = []

                            for c in groupby_cols:
                                if c == probe_key:
                                    key_tuples.append((c, probe_key_ir))
                                elif c == part_key:
                                    key_tuples.append((c, probe_key_ir))
                                elif c in probe_on.columns:
                                    key_tuples.append((c, self.probe_access(c)))
                                elif c in part_on.columns:
                                    key_tuples.append((c, self.part_lookup(c)))
                                else:
                                    raise IndexError(f'Cannot find such a column {c} '
                                                     f'in part side {part_on.name} '
                                                     f'and probe side {probe_on.name}')

                            dict_key_ir = RecConsExpr(key_tuples)

                        if len(aggr_dict.keys()) == 0:
                            raise ValueError()
                        elif len(aggr_dict.keys()) == 1:
                            '''
                            Aggregation as a single value
                            Then format to a singleton dictionary
                            '''

                            dict_val = list(aggr_dict.items())[0][1]

                            if isinstance(dict_val, RecAccessExpr):
                                # (, 'sum')
                                dict_val_name = dict_val.name

                                if dict_val_name in probe_on.columns:
                                    aggr_body = DicConsExpr([(dict_key_ir,
                                                              probe_on.key_access(dict_val_name))])
                                else:
                                    if dict_val_name in joint_col_ins.keys():
                                        if any([i not in probe_on.columns for i in self.retriever.find_cols(joint_col_ins[dict_val_name])]):
                                            col_mapper = {}
                                            for j in self.retriever.find_cols(joint_col_ins[dict_val_name]):
                                                col_mapper[j] = self.retriever.find_lookup_path(self, j)

                                            aggr_body = DicConsExpr([(dict_key_ir,
                                                                      joint_col_ins[dict_val_name].replace(
                                                                          rec=None,
                                                                          inplace=True,
                                                                          mapper=col_mapper
                                                                      ))])
                                        else:
                                            aggr_body = DicConsExpr([(dict_key_ir,
                                                                      joint_col_ins[dict_val_name].replace(
                                                                          probe_on.iter_el.key))])
                                    else:
                                        raise IndexError(
                                            f'Cannot find column {dict_val_name} in {probe_on.columns}')
                            elif isinstance(dict_val, ConstantExpr):
                                # (, 'count')
                                aggr_body = DicConsExpr([(dict_key_ir,
                                                          dict_val)])
                            else:
                                raise NotImplementedError
                        else:
                            '''
                            Aggregation as a single record
                            Then format to a singleton dictionary
                            '''
                            val_tuples = []
                            for k in aggr_dict.keys():
                                v = aggr_dict[k]

                                if isinstance(v, RecAccessExpr):
                                    # (, 'sum')
                                    v_name = v.name

                                    if v_name in probe_on.columns:
                                        val_tuples.append((k, probe_on.key_access(v_name)))
                                    else:
                                        if v_name in joint_col_ins.keys():
                                            val_tuples.append((k,
                                                               joint_col_ins[v_name].replace(probe_on.iter_el.key)))
                                        else:
                                            raise IndexError(f'Cannot find column {v_name} in {probe_on.columns}')
                                elif isinstance(v, ConstantExpr):
                                    # (, 'count')
                                    val_tuples.append((k, v))
                                else:
                                    raise NotImplementedError

                            aggr_body = DicConsExpr([(dict_key_ir, RecConsExpr(val_tuples))])

                        # print({
                        #     'name': self.joint.name,
                        #     'expr': aggr_body,
                        # })

                        # probe condition: first outermost layer
                        if probe_cond:
                            cond_cols = self.retriever.find_cond_on(probe_cond, {True: tuple(aggr_dict.keys())})
                            if cond_cols:
                                if all(cond_cols):
                                    # This is to drop the condition that is ONLY for the previous aggregation
                                    pass
                            else:
                                aggr_body = IfExpr(condExpr=probe_cond.sdql_ir,
                                                   thenBodyExpr=aggr_body,
                                                   elseBodyExpr=ConstantExpr(None))

                        # joint condition: second outermost layer
                        if joint_cond:
                            aggr_body = IfExpr(condExpr=joint_cond,
                                               thenBodyExpr=aggr_body,
                                               elseBodyExpr=ConstantExpr(None))

                        # part non null condition: add to the inner layer
                        aggr_body = SDQLInspector.add_cond(aggr_body,
                                                           self.part_nonull(),
                                                           'inner')

                        # isin non null condition: add to the inner layer
                        if probe_isin_expr:
                            aggr_body = SDQLInspector.add_cond(aggr_body,
                                                               probe_isin_expr.get_as_cond(),
                                                               'inner')

                        aggr_sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                                dictExpr=probe_on.var_expr,
                                                bodyExpr=aggr_body,
                                                isAssignmentSum=False)

                        vname_aggr = f'{probe_on.name}_aggr'
                        var_aggr = VarExpr(vname_aggr)
                        probe_on.add_context_variable(vname_aggr,
                                                      var_aggr)

                        aggr_let_expr = LetExpr(varExpr=var_aggr,
                                                valExpr=aggr_sum_expr,
                                                bodyExpr=ConstantExpr(True))

                        vname_x_aggr = f'x_{vname_aggr}'
                        var_x_aggr = VarExpr(vname_x_aggr)
                        probe_on.add_context_variable(vname_x_aggr,
                                                      var_x_aggr)

                        # aggr = {? : scalar}
                        if len(aggr_dict.keys()) == 1:
                            dict_key = list(aggr_dict.items())[0][0]

                            format_key_tuples = []

                            # aggr = {scalar : scalar}
                            if len(groupby_cols) == 1:
                                format_key_tuples.append((groupby_cols[0],
                                                          PairAccessExpr(var_x_aggr, 0)))
                            # aggr = {record : scalar}
                            else:
                                for c in groupby_cols:
                                    format_key_tuples.append((c, RecAccessExpr(PairAccessExpr(var_x_aggr, 0), c)))

                            format_key_tuples.append((dict_key, PairAccessExpr(var_x_aggr, 1)))

                            format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                                      ConstantExpr(True))])
                        # aggr = {? : record}
                        else:
                            # aggr = {scalar: record}
                            if len(groupby_cols) == 1:
                                format_key_tuples = [(groupby_cols[0],
                                                      PairAccessExpr(var_x_aggr, 0))]

                                for k in aggr_dict.keys():
                                    format_key_tuples.append((k,
                                                              RecAccessExpr(PairAccessExpr(var_x_aggr,
                                                                                           1),
                                                                            k)))

                                format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                                          ConstantExpr(True))])
                            else:
                                # aggr = {record : record}
                                format_op = DicConsExpr([(ConcatExpr(PairAccessExpr(var_x_aggr, 0),
                                                                     PairAccessExpr(var_x_aggr, 1)),
                                                          ConstantExpr(True))])

                        format_sum = SumExpr(varExpr=var_x_aggr,
                                             dictExpr=var_aggr,
                                             bodyExpr=format_op,
                                             isAssignmentSum=True)

                        vname_res = f'results'
                        var_res = VarExpr(vname_res)
                        probe_on.add_context_variable(vname_res,
                                                      var_res)

                        form_let_expr = LetExpr(varExpr=var_res,
                                                valExpr=format_sum,
                                                bodyExpr=ConstantExpr(True))

                        if probe_isin_expr:
                            isin_let_expr = probe_isin_expr.get_as_part()
                            return SDQLInspector.concat_bindings([isin_let_expr, aggr_let_expr, form_let_expr])
                        else:
                            return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])

                    else:
                        # part side is NOT joint
                        # probe side is NOT joint
                        # part side could NOT have isin()
                        # probe side could have isin()

                        # Q12
                        # Q13
                        # Q16

                        groupby_aggr_info = self.retriever.find_groupby_aggr()

                        aggr_dict = groupby_aggr_info.aggr_dict
                        groupby_cols = groupby_aggr_info.groupby_cols

                        joint_cond = self.retriever.find_cond_before(GroupbyAggrExpr)
                        joint_col_ins = self.retriever.find_col_ins_before(GroupbyAggrExpr)
                        probe_isin_expr = self.probe_frame.retriever.find_isin_before(MergeExpr)

                        if len(groupby_cols) == 0:
                            raise ValueError()
                        elif len(groupby_cols) == 1:
                            only_key_col = groupby_cols[0]
                            if only_key_col in probe_on.columns:
                                dict_key_ir = probe_on.key_access(only_key_col)
                            elif only_key_col in part_on.columns:
                                dict_key_ir = self.part_lookup(only_key_col)
                            elif self.retriever.has_multi_gourpby_aggr:
                                part_groupby_aggr_expr = self.retriever.find_groupby_aggr_after(MergeExpr)

                                if self.retriever.find_merge('as_joint').how == 'right':
                                    aggr_val_expr = part_groupby_aggr_expr.origin_dict[only_key_col]
                                    if isinstance(aggr_val_expr, str):
                                        if aggr_val_expr == 'sum':
                                            else_value = ConstantExpr(0.0)
                                        elif aggr_val_expr == 'count':
                                            else_value = ConstantExpr(0)
                                        else:
                                            raise ValueError(f'Aggregation function {aggr_val_expr} is not supported.')
                                    else:
                                        if aggr_val_expr[1] == 'sum':
                                            else_value = ConstantExpr(0.0)
                                        elif aggr_val_expr[1] == 'count':
                                            else_value = ConstantExpr(0)
                                        else:
                                            raise ValueError(
                                                f'Aggregation function {aggr_val_expr[1]} is not supported.')

                                    dict_key_ir = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                              self.part_lookup(),
                                                                              ConstantExpr(None)),
                                                         thenBodyExpr=self.part_lookup(only_key_col),
                                                         elseBodyExpr=else_value)
                                else:
                                    raise NotImplementedError
                            else:
                                raise IndexError(f'Column {groupby_cols[0]} not found!')
                        else:
                            key_tuples = []

                            for c in groupby_cols:
                                if c in probe_on.columns:
                                    key_tuples.append((c, self.probe_access(c)))
                                elif c in part_on.columns:
                                    key_tuples.append((c, self.part_lookup(c)))
                                else:
                                    raise IndexError(f'Cannot find such a column {c} '
                                                     f'in part side {part_on.name} '
                                                     f'and probe side {probe_on.name}')

                            dict_key_ir = RecConsExpr(key_tuples)

                        if len(aggr_dict.keys()) == 0:
                            raise ValueError()
                        elif len(aggr_dict.keys()) == 1:
                            '''
                            Aggregation as a single value
                            Then format to a singleton dictionary
                            '''

                            dict_val = list(aggr_dict.items())[0][1]

                            if isinstance(dict_val, RecAccessExpr):
                                # (, 'sum')
                                dict_val_name = dict_val.name

                                if dict_val_name in probe_on.columns:
                                    aggr_body = DicConsExpr([(dict_key_ir,
                                                              probe_on.key_access(dict_val_name))])
                                else:
                                    if dict_val_name in joint_col_ins.keys():
                                        aggr_body = DicConsExpr([(dict_key_ir,
                                                                  joint_col_ins[dict_val_name].replace(
                                                                      probe_on.iter_el.key))])
                                    else:
                                        raise IndexError(
                                            f'Cannot find column {dict_val_name} in {probe_on.columns}')
                            elif isinstance(dict_val, ConstantExpr):
                                # (, 'count')
                                aggr_body = DicConsExpr([(dict_key_ir,
                                                          dict_val)])
                            else:
                                raise NotImplementedError
                        else:
                            '''
                            Aggregation as a single record
                            Then format to a singleton dictionary
                            '''
                            val_tuples = []
                            for k in aggr_dict.keys():
                                v = aggr_dict[k]

                                if isinstance(v, RecAccessExpr):
                                    # (, 'sum')
                                    v_name = v.name

                                    if v_name in probe_on.columns:
                                        val_tuples.append((k, probe_on.key_access(v_name)))
                                    else:
                                        if v_name in joint_col_ins.keys():
                                            new_col = joint_col_ins[v_name]
                                            if isinstance(new_col,
                                                          (ColEl, ColOpExpr, ColExtExpr, NewColOpExpr, OldColOpExpr)):
                                                val_tuples.append((k,
                                                                   new_col.replace(probe_on.iter_el.key)))
                                            elif isinstance(new_col, IfExpr):
                                                val_tuples.append((k,
                                                                   new_col))
                                            else:
                                                raise NotImplementedError
                                        else:
                                            raise IndexError(f'Cannot find column {v_name} in {probe_on.columns}')
                                elif isinstance(v, ConstantExpr):
                                    # (, 'count')
                                    val_tuples.append((k, v))
                                else:
                                    raise NotImplementedError

                            aggr_body = DicConsExpr([(dict_key_ir, RecConsExpr(val_tuples))])

                        if probe_cond:
                            aggr_body = IfExpr(condExpr=probe_cond.sdql_ir,
                                               thenBodyExpr=aggr_body,
                                               elseBodyExpr=ConstantExpr(None))

                        # joint condition: outermost layer
                        join_how = self.retriever.find_merge('as_joint').how
                        if joint_cond:
                            aggr_body = IfExpr(condExpr=joint_cond,
                                               thenBodyExpr=aggr_body,
                                               elseBodyExpr=ConstantExpr(None))

                        if join_how == 'inner':
                            # part non null condition: add to the inner layer
                            aggr_body = SDQLInspector.add_cond(aggr_body,
                                                               self.part_nonull(),
                                                               'inner')

                        # isin non null condition: add to the inner layer
                        if probe_isin_expr:
                            aggr_body = SDQLInspector.add_cond(aggr_body,
                                                               probe_isin_expr.get_as_cond(),
                                                               'inner')

                        aggr_sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                                dictExpr=probe_on.var_expr,
                                                bodyExpr=aggr_body,
                                                isAssignmentSum=False)

                        vname_aggr = f'{probe_on.name}_aggr'
                        var_aggr = VarExpr(vname_aggr)
                        probe_on.add_context_variable(vname_aggr,
                                                      var_aggr)

                        aggr_let_expr = LetExpr(varExpr=var_aggr,
                                                valExpr=aggr_sum_expr,
                                                bodyExpr=ConstantExpr(True))

                        vname_x_aggr = f'x_{vname_aggr}'
                        var_x_aggr = VarExpr(vname_x_aggr)
                        probe_on.add_context_variable(vname_x_aggr,
                                                      var_x_aggr)

                        # aggr = {? : scalar}
                        if len(aggr_dict.keys()) == 1:
                            dict_key = list(aggr_dict.items())[0][0]

                            format_key_tuples = []

                            # aggr = {scalar : scalar}
                            if len(groupby_cols) == 1:
                                format_key_tuples.append((groupby_cols[0],
                                                          PairAccessExpr(var_x_aggr, 0)))
                            # aggr = {record : scalar}
                            else:
                                for c in groupby_cols:
                                    format_key_tuples.append((c, RecAccessExpr(PairAccessExpr(var_x_aggr, 0), c)))

                            format_key_tuples.append((dict_key, PairAccessExpr(var_x_aggr, 1)))

                            format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                                      ConstantExpr(True))])
                        # aggr = {? : record}
                        else:
                            # aggr = {scalar: record}
                            if len(groupby_cols) == 1:
                                format_key_tuples = [(groupby_cols[0],
                                                      PairAccessExpr(var_x_aggr, 0))]

                                for k in aggr_dict.keys():
                                    format_key_tuples.append((k,
                                                              RecAccessExpr(PairAccessExpr(var_x_aggr,
                                                                                           1),
                                                                            k)))

                                format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                                          ConstantExpr(True))])
                            else:
                                # aggr = {record : record}
                                format_op = DicConsExpr([(ConcatExpr(PairAccessExpr(var_x_aggr, 0),
                                                                     PairAccessExpr(var_x_aggr, 1)),
                                                          ConstantExpr(True))])

                        format_sum = SumExpr(varExpr=var_x_aggr,
                                             dictExpr=var_aggr,
                                             bodyExpr=format_op,
                                             isAssignmentSum=True)

                        vname_res = f'results'
                        var_res = VarExpr(vname_res)
                        probe_on.add_context_variable(vname_res,
                                                      var_res)

                        form_let_expr = LetExpr(varExpr=var_res,
                                                valExpr=format_sum,
                                                bodyExpr=ConstantExpr(True))

                        if probe_isin_expr:
                            isin_let_expr = probe_isin_expr.get_as_part()
                            return SDQLInspector.concat_bindings([isin_let_expr, aggr_let_expr, form_let_expr])
                        else:
                            return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])

            # Q2
            # Q15
            # Q20
            if self.retriever.last_iter_is_merge:
                if self.probe_frame.retriever.was_aggregated:
                    # Q15

                    key_rec_list = []

                    if self.probe_frame.retriever.was_aggr:
                        raise NotImplementedError
                    elif self.probe_frame.retriever.was_groupby_aggr:
                        groupby_aggr_info = self.probe_frame.retriever.find_groupby_aggr()

                        groupby_cols = groupby_aggr_info.groupby_cols
                        aggr_dict = groupby_aggr_info.aggr_dict

                        vname_aggr = f'{self.probe_frame.probe_on.name}_aggr'
                        var_aggr = VarExpr(vname_aggr)
                        vname_x_aggr = f'x_{vname_aggr}'
                        var_x_aggr = VarExpr(vname_x_aggr)

                        if isinstance(part_key, str) and isinstance(probe_key, str):
                            if joint_col_proj:
                                cleaned_col_proj = []

                                [cleaned_col_proj.append(x)
                                 for x in joint_col_proj.proj_cols
                                 if x not in cleaned_col_proj]

                                # aggr = {? : scalar}
                                if len(aggr_dict.keys()) == 1:
                                    # aggr = {scalar : scalar}
                                    if len(groupby_cols) == 1:
                                        if groupby_cols[0] != probe_key:
                                            raise IndexError(f'Cannot find column {probe_key} '
                                                             f'in groupby columns {groupby_cols}')
                                        else:
                                            for col in cleaned_col_proj:
                                                # as left_on
                                                if col == part_key:
                                                    key_rec_list.append((col,
                                                                         PairAccessExpr(var_x_aggr, 0)))
                                                # as right_on
                                                elif col == probe_key:
                                                    key_rec_list.append((col,
                                                                         PairAccessExpr(var_x_aggr, 0)))
                                                # as val of dict (groupby aggr dict)
                                                elif col == list(aggr_dict.keys())[0]:
                                                    key_rec_list.append((col,
                                                                         PairAccessExpr(var_x_aggr, 1)))
                                                # from part side
                                                elif col in self.part_frame.cols_out:
                                                    key_rec_list.append((col,
                                                                         RecAccessExpr(
                                                                             DicLookupExpr(part_var,
                                                                                           PairAccessExpr(
                                                                                               var_x_aggr,
                                                                                               0)
                                                                                           ),
                                                                             col)
                                                                         ))
                                                else:
                                                    raise IndexError(f'Column {col} not found')

                                            aggr_key_ir = RecConsExpr(key_rec_list)

                                            aggr_body = DicConsExpr([(aggr_key_ir, ConstantExpr(True))])

                                            cond_after_aggr = self.probe_frame.retriever.find_cond_after(
                                                GroupbyAggrExpr)

                                            if cond_after_aggr:
                                                cond_after_aggr = cond_after_aggr.replace(
                                                    rec=None,
                                                    inplace=True,
                                                    mapper={list(aggr_dict.keys())[0]: PairAccessExpr(var_x_aggr, 1)})

                                                aggr_body = IfExpr(condExpr=cond_after_aggr,
                                                                   thenBodyExpr=aggr_body,
                                                                   elseBodyExpr=ConstantExpr(None))

                                            aggr_body = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                                    DicLookupExpr(
                                                                                        dicExpr=self.part_frame.part_var,
                                                                                        keyExpr=PairAccessExpr(
                                                                                            var_x_aggr,
                                                                                            0)),
                                                                                    ConstantExpr(None)),
                                                               thenBodyExpr=aggr_body,
                                                               elseBodyExpr=ConstantExpr(None))

                                            sum_expr = SumExpr(varExpr=var_x_aggr,
                                                               dictExpr=var_aggr,
                                                               bodyExpr=aggr_body,
                                                               isAssignmentSum=True)

                                            var_res = VarExpr('results')
                                            self.joint.add_context_variable('results', var_res)

                                            out = LetExpr(var_res, sum_expr, ConstantExpr(True))

                                            return out

                                    # aggr = {record : scalar}
                                    else:
                                        raise NotImplementedError
                                # aggr = {? : record}
                                else:
                                    # aggr = {scalar: record}
                                    if len(groupby_cols) == 1:
                                        raise NotImplementedError
                                    # aggr = {record : record}
                                    else:
                                        raise NotImplementedError
                            else:
                                raise NotImplementedError
                        elif isinstance(part_key, list) and isinstance(probe_key, list):
                            print(part_key, probe_key)

                            raise NotImplementedError
                        else:
                            raise NotImplementedError

                elif self.probe_frame.retriever.is_joint:
                    # Q2

                    all_part_sides = self.retriever.findall_part_for_root_probe('as_body')

                    root_probe_side = self.retriever.find_root_probe()

                    root_isin = root_probe_side.retriever.find_isin()

                    dict_key_list = []

                    joint_col_proj = self.retriever.find_col_proj()

                    if joint_col_proj:
                        joint_col_proj = joint_col_proj.proj_cols

                        for c in joint_col_proj:
                            if c == probe_key:
                                dict_key_list.append((c, root_probe_side.key_access(probe_key)))
                            elif c == part_key:
                                dict_key_list.append((c, root_probe_side.key_access(probe_key)))
                            elif c in root_probe_side.columns:
                                dict_key_list.append((c, root_probe_side.key_access(c)))
                            else:
                                c_found = False
                                for this_part_side in all_part_sides:
                                    if c_found:
                                        continue

                                    if c in this_part_side.columns:
                                        this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                                        if isinstance(this_probe_key, list):
                                            key_rec = RecConsExpr([(c, this_part_side.key_access(c))
                                                                   for c in this_probe_key])
                                            dict_key_list.append((c, RecAccessExpr(
                                                recExpr=DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                      keyExpr=key_rec),
                                                fieldName=c)))
                                        else:
                                            dict_key_list.append((c, RecAccessExpr(
                                                recExpr=DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                      keyExpr=root_probe_side.key_access(
                                                                          this_probe_key)),
                                                fieldName=c
                                            )))
                                        c_found = True

                                if not c_found:
                                    raise ValueError(f'Not found {c}')

                    dict_key_ir = RecConsExpr(dict_key_list)

                    joint_op = DicConsExpr([(dict_key_ir, ConstantExpr(True))])

                    if joint_cond:
                        cond_mapper = {}
                        for this_part_side in all_part_sides:
                            this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                            cond_mapper[tuple(this_part_side.cols_out)] = DicLookupExpr(
                                dicExpr=this_part_side.get_var_part(),
                                keyExpr=root_probe_side.key_access(this_probe_key))

                        if cond_mapper:
                            joint_cond_ir = joint_cond.replace(rec=None, inplace=False, mapper=cond_mapper)
                        else:
                            joint_cond_ir = joint_cond.sdql_ir

                        joint_op = IfExpr(condExpr=joint_cond_ir,
                                          thenBodyExpr=joint_op,
                                          elseBodyExpr=ConstantExpr(None))

                    for this_part_side in all_part_sides:
                        this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                        if this_probe_key in root_probe_side.columns:
                            joint_op = IfExpr(
                                condExpr=CompareExpr(CompareSymbol.NE,
                                                     DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                   keyExpr=root_probe_side.key_access(
                                                                       this_probe_key)),
                                                     ConstantExpr(None)),
                                thenBodyExpr=joint_op,
                                elseBodyExpr=ConstantExpr(None)
                            )
                        else:
                            lookup_key = self.retriever.find_lookup_path(self, this_probe_key)
                            joint_op = IfExpr(
                                condExpr=CompareExpr(CompareSymbol.NE,
                                                     DicLookupExpr(dicExpr=this_part_side.get_var_part(),
                                                                   keyExpr=lookup_key),
                                                     ConstantExpr(None)),
                                thenBodyExpr=joint_op,
                                elseBodyExpr=ConstantExpr(None)
                            )

                    if root_isin:
                        joint_op = IfExpr(condExpr=root_isin.get_as_cond(),
                                          thenBodyExpr=joint_op,
                                          elseBodyExpr=ConstantExpr(None))

                    # print(probe_on.retriever.find_cond())

                    joint_sum = SumExpr(varExpr=root_probe_side.iter_el.sdql_ir,
                                        dictExpr=root_probe_side.var_expr,
                                        bodyExpr=joint_op,
                                        isAssignmentSum=False)

                    var_res = VarExpr('results')
                    self.joint.add_context_variable('results', var_res)

                    if root_isin:
                        joint_let = LetExpr(var_res, joint_sum, ConstantExpr(True))

                        out = root_isin.get_as_part(joint_let)
                    else:
                        out = LetExpr(var_res, joint_sum, ConstantExpr(True))

                    return out

                elif self.probe_frame.retriever.was_isin:
                    # Q20

                    probe_isin = self.probe_frame.retriever.find_isin()

                    dict_key_list = []

                    joint_col_proj = self.retriever.find_col_proj()

                    if joint_col_proj:
                        joint_col_proj = joint_col_proj.proj_cols

                        for c in joint_col_proj:
                            dict_key_list.append((c, self.probe_access(c)))

                    dict_key_ir = RecConsExpr(dict_key_list)

                    joint_op = DicConsExpr([(dict_key_ir, ConstantExpr(True))])

                    joint_op = IfExpr(condExpr=self.part_nonull(),
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=ConstantExpr(None))

                    if probe_isin:
                        joint_op = IfExpr(condExpr=probe_isin.get_as_cond(),
                                          thenBodyExpr=joint_op,
                                          elseBodyExpr=ConstantExpr(None))

                    joint_sum = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                        dictExpr=probe_on.var_expr,
                                        bodyExpr=joint_op,
                                        isAssignmentSum=False)

                    var_res = VarExpr('results')
                    self.joint.add_context_variable('results', var_res)

                    if probe_isin:
                        joint_let = LetExpr(var_res, joint_sum, ConstantExpr(True))
                        out = probe_isin.get_as_part(joint_let)
                    else:
                        out = LetExpr(var_res, joint_sum, ConstantExpr(True))

                    return out
                else:
                    if isinstance(self.part_frame.get_part_key(), list) and \
                            isinstance(self.probe_frame.get_probe_key(), list):
                        # Q20

                        # aggr_key_ir
                        key_rec_list = []

                        for c in self.retriever.findall_cols_used(only_next=True):
                            if c in part_key:
                                icol = part_key.index(c)
                                key_rec_list.append((c, self.probe_access(probe_key[icol])))
                        if len(key_rec_list) == 0:
                            raise NotImplementedError
                        elif len(key_rec_list) == 1:
                            aggr_key_ir = key_rec_list[0][1]
                        else:
                            aggr_key_ir = RecConsExpr(key_rec_list)

                        joint_op = DicConsExpr([(aggr_key_ir, ConstantExpr(True))])

                        if joint_cond:
                            cond_mapper = {}
                            for c in self.part_frame.cols_out:
                                cond_mapper[c] = self.part_lookup(c)
                            joint_op = IfExpr(condExpr=joint_cond.replace(rec=None, inplace=True, mapper=cond_mapper),
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                        joint_op = IfExpr(
                            condExpr=CompareExpr(CompareSymbol.NE,
                                                 self.part_lookup(),
                                                 ConstantExpr(None)),
                            thenBodyExpr=joint_op,
                            elseBodyExpr=ConstantExpr(None)
                        )

                        if probe_cond:
                            joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                        joint_sum = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                            dictExpr=probe_on.var_expr,
                                            bodyExpr=joint_op,
                                            isAssignmentSum=False)

                        if next_op:
                            out = LetExpr(self.joint.var_expr, joint_sum, next_op)
                        else:
                            var_res = VarExpr('results')
                            self.joint.add_context_variable('results', var_res)

                            out = LetExpr(var_res, joint_sum, ConstantExpr(True))

                        return out

                    if isinstance(self.part_frame.get_part_key(), str) \
                            and isinstance(self.probe_frame.get_probe_key(), str):

                        # aggr_key_ir
                        key_rec_list = []

                        cleaned_col_proj = []

                        [cleaned_col_proj.append(x)
                         for x in sorted(self.col_proj)
                         if x not in cleaned_col_proj]

                        for col in cleaned_col_proj:
                            col_op = col[0]
                            if col_op in self.part_frame.cols_out:
                                key_rec_list.append((col_op,
                                                     self.part_lookup(col_op)))

                        aggr_key_ir = RecConsExpr(key_rec_list)

                        joint_op = IfExpr(
                            condExpr=CompareExpr(CompareSymbol.NE,
                                                 DicLookupExpr(dicExpr=part_var,
                                                               keyExpr=probe_on.key_access(
                                                                   probe_key)),
                                                 ConstantExpr(None)),
                            thenBodyExpr=DicConsExpr([(aggr_key_ir, ConstantExpr(True))]),
                            elseBodyExpr=ConstantExpr(None)
                        )

                        if probe_cond:
                            joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                        joint_sum = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                            dictExpr=probe_on.var_expr,
                                            bodyExpr=joint_op,
                                            isAssignmentSum=False)

                        var_res = VarExpr('results')
                        self.joint.add_context_variable('results', var_res)

                        out = LetExpr(var_res, joint_sum, ConstantExpr(True))

                        return out

            if self.retriever.last_iter_is_calc:
                # Q14
                # Q17
                rec_list = []

                col_inserted = self.retriever.findall_col_insert()

                for i in self.retriever.findall_col_insert().keys():
                    if isinstance(col_inserted[i], IfExpr):
                        rec_list.append((i, col_inserted[i]))
                    elif isinstance(col_inserted[i], FlexIR):
                        rec_list.append((i, col_inserted[i].sdql_ir))
                    else:
                        raise TypeError(f'Unsupported Type {type(col_inserted[i])}')

                col_renamed = self.retriever.findall_col_rename()

                for j in self.retriever.findall_col_rename().keys():
                    if isinstance(col_renamed[j], IfExpr):
                        rec_list.append((j, col_renamed[j]))
                    elif isinstance(col_renamed[j], FlexIR):
                        rec_list.append((j, col_renamed[j].sdql_ir))
                    else:
                        raise TypeError(f'Unsupported Type {type(col_renamed[j])}')

                rec = RecConsExpr(rec_list)

                if probe_cond:
                    rec = IfExpr(condExpr=probe_cond.sdql_ir,
                                 thenBodyExpr=rec,
                                 elseBodyExpr=ConstantExpr(None))

                sum_expr = SumExpr(varExpr=self.probe_frame.probe_on.iter_el.el,
                                   dictExpr=self.probe_frame.get_probe_on_var(),
                                   bodyExpr=rec,
                                   isAssignmentSum=False)

                calc_expr = self.retriever.find_calc().sdql_ir

                var_res = VarExpr('results')
                self.joint.add_context_variable('results', var_res)

                out = LetExpr(varExpr=probe_var,
                              valExpr=sum_expr,
                              bodyExpr=LetExpr(var_res, calc_expr, ConstantExpr(True)))

                return out

        else:
            # Q2
            # Q5
            # Q7
            # Q8
            # Q10
            # Q17
            # Q18
            # Q21
            if self.retriever.as_part_for_next_join:
                if self.probe_frame.retriever.is_joint:
                    last_merge_expr = self.retriever.find_merge(mode='as_joint')
                    next_merge_expr = self.retriever.find_merge(mode='as_part')

                    all_part_sides = self.retriever.findall_part_for_root_probe('as_body')

                    root_merge = self.retriever.find_root_merge()
                    root_part_side = root_merge.left
                    root_probe_side = root_merge.right
                    root_part_key = root_merge.left_on
                    root_probe_key = root_merge.right_on

                    # dict key (single)

                    key_col = next_merge_expr.left_on

                    if isinstance(key_col, list):
                        dict_key_ir = RecConsExpr([(c, root_probe_side.key_access(c))
                                                   for c in key_col])
                    else:
                        if key_col in root_probe_side.columns:
                            dict_key_ir = root_probe_side.key_access(key_col)
                        else:
                            dict_key_ir = self.retriever.find_bypass_lookup(all_part_sides, key_col, root_merge)

                    # dict vals
                    if self.col_proj:
                        val_cols = [i[0] for i in self.col_proj]
                    else:
                        val_cols = [x for x in self.retriever.findall_cols_used()
                                    if (x != last_merge_expr.left_on
                                        and x != last_merge_expr.right_on
                                        and x != key_col
                                        and x not in self.retriever.find_renamed_cols())
                                    or (x in self.retriever.find_cols_used('merge')
                                        and x != key_col
                                        and x not in self.retriever.find_renamed_cols())]

                    dict_val_list = []

                    for col in val_cols:
                        if col == root_part_key:
                            dict_val_list.append((
                                col,
                                root_probe_side.key_access(root_probe_key)
                            ))
                        elif col == root_probe_key:
                            dict_val_list.append((
                                col,
                                root_probe_side.key_access(root_probe_key)
                            ))
                        elif col in root_part_side.columns:
                            dict_val_list.append((
                                col,
                                RecAccessExpr(recExpr=DicLookupExpr(dicExpr=root_part_side.var_part,
                                                                    keyExpr=root_probe_side.key_access(
                                                                        root_probe_key)),
                                              fieldName=col)
                            ))
                        elif col in root_probe_side.columns:
                            dict_val_list.append((
                                col,
                                root_probe_side.key_access(col)
                            ))
                        else:
                            for part in all_part_sides:
                                if col in part.columns:
                                    dict_val_list.append((
                                        col,
                                        self.retriever.find_bypass_lookup(all_part_sides, col, root_merge)
                                    ))

                    dict_val_ir = RecConsExpr(dict_val_list)

                    # print({
                    #     'part': [i.name for i in all_part_sides],
                    #     'probe': root_probe_side.name,
                    #     'key': dict_key_ir,
                    #     'val': dict_val_ir
                    # })

                    joint_op = DicConsExpr([(dict_key_ir, dict_val_ir)])

                    if joint_cond:
                        cols_not_in_root_probe = [x for x in self.retriever.find_cols(joint_cond)
                                                  if x not in root_probe_side.columns]
                        if cols_not_in_root_probe:
                            cond_replace_mapper = {}
                            # If found some columns are not in the root probe side
                            for c in cols_not_in_root_probe:
                                for this_part_side in all_part_sides:
                                    if c in this_part_side.columns:
                                        this_probe_key = this_part_side.get_retriever().find_probe_key_as_part_side()
                                        cond_replace_mapper[c] = DicLookupExpr(
                                            dicExpr=this_part_side.get_var_part(),
                                            keyExpr=root_probe_side.key_access(this_probe_key))

                            joint_op = IfExpr(condExpr=self.retriever.replace_cond(cond=joint_cond,
                                                                                   mapper=cond_replace_mapper).sdql_ir,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))
                        else:
                            joint_op = IfExpr(condExpr=joint_cond,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                    joint_op = IfExpr(
                        condExpr=CompareExpr(CompareSymbol.NE,
                                             DicLookupExpr(dicExpr=root_part_side.get_var_part(),
                                                           keyExpr=root_probe_side.key_access(
                                                               root_probe_key)),
                                             ConstantExpr(None)),
                        thenBodyExpr=joint_op,
                        elseBodyExpr=ConstantExpr(None))

                    probe_side_conds = root_probe_side.retriever.findall_cond()
                    if probe_side_conds:
                        for probe_cond in probe_side_conds:
                            joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                    joint_sum = SumExpr(varExpr=root_probe_side.iter_el.sdql_ir,
                                        dictExpr=root_probe_side.var_expr,
                                        bodyExpr=joint_op,
                                        isAssignmentSum=False)

                    joint_let = LetExpr(varExpr=self.joint.var_expr,
                                        valExpr=joint_sum,
                                        bodyExpr=next_op)

                    return joint_let

                # probe side is not joint
                else:
                    # Q2
                    # Q7
                    # Q8
                    # Q17
                    # Q18
                    # Q21

                    last_merge_expr = self.retriever.find_merge(mode='as_joint')
                    next_merge_expr = self.retriever.find_merge(mode='as_part')

                    probe_isin = self.probe_frame.retriever.find_isin()

                    renamed_cols = self.probe_frame.retriever.findall_col_rename(reverse=True)

                    if renamed_cols:
                        if probe_key in renamed_cols.keys():
                            probe_key = renamed_cols[probe_key]
                            probe_key_ir = RecAccessExpr(probe_on.iter_el.key, probe_key)

                    # dict key: single column
                    if isinstance(next_merge_expr.left_on, list):
                        if len(next_merge_expr.left_on) == 1:
                            key_col = next_merge_expr.left_on[0]
                        else:
                            key_col = next_merge_expr.left_on

                            if renamed_cols:
                                for k in key_col:
                                    if k in renamed_cols.keys():
                                        key_col[key_col.index(k)] = renamed_cols[k]

                            dict_key_cols = []

                            for k in key_col:
                                if k in self.probe_frame.probe_on.columns:
                                    dict_key_cols.append((k, self.probe_access(k)))
                                else:
                                    dict_key_cols.append((k, self.retriever.find_lookup_path(self, k)))

                            dict_key_ir = RecConsExpr(dict_key_cols)

                            # dict vals: multi(single) column

                            val_cols = [x for x in self.retriever.findall_cols_used()
                                        if (x != last_merge_expr.left_on
                                            and x != last_merge_expr.right_on
                                            and x not in key_col
                                            and x not in self.retriever.find_renamed_cols())
                                        or (x in self.retriever.find_cols_used('merge')
                                            and x not in key_col
                                            and x not in self.retriever.find_renamed_cols())
                                        or (x in self.retriever.findall_cols_for_groupby_aggr())]

                            if not val_cols:
                                if joint_col_proj:
                                    for i in joint_col_proj.proj_cols:
                                        val_cols.append(i)

                            dict_val_list = []

                            for i in val_cols:
                                if i == part_key:
                                    dict_val_list.append((i,
                                                          probe_key_ir))
                                elif i == probe_key:
                                    dict_val_list.append((i,
                                                          probe_key_ir))
                                elif i in self.part_frame.part_on.columns:
                                    if i in renamed_cols.keys():
                                        dict_val_list.append((renamed_cols[i],
                                                              RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                                  keyExpr=probe_on.key_access(
                                                                                                      probe_key)),
                                                                            fieldName=renamed_cols[i])))
                                    else:
                                        dict_val_list.append((i,
                                                              RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                                  keyExpr=probe_on.key_access(
                                                                                                      probe_key)),
                                                                            fieldName=i)))
                                elif i in self.probe_frame.probe_on.columns:
                                    # print(i, 'in', self.probe_frame.probe_on, 'with', self.probe_frame.probe_on.columns)
                                    dict_val_list.append((i,
                                                          probe_on.key_access(i)))

                                elif i in self.retriever.find_renamed_cols(mode='as_val'):
                                    origin_col = self.retriever.find_col_rename(col_name=i, by='val')

                                    if origin_col in probe_on.columns:
                                        dict_val_list.append((i,
                                                              probe_on.key_access(origin_col)))
                                    else:
                                        dict_val_list.append((i,
                                                              self.part_lookup(origin_col)))

                                elif self.probe_frame.retriever.was_groupby_aggr:
                                    groupby_aggr_expr = self.probe_frame.retriever.find_groupby_aggr()
                                    groupby_cols = groupby_aggr_expr.groupby_cols
                                    aggr_dict = groupby_aggr_expr.origin_dict

                                    if i in groupby_cols:
                                        dict_val_list.append((i,
                                                              self.probe_access(i)))
                                    elif i in aggr_dict.keys():
                                        if isinstance(aggr_dict[i], tuple):
                                            if aggr_dict[i][1] == 'sum':
                                                dict_val_list.append((i,
                                                                      self.probe_access(aggr_dict[i][0])))
                                            elif aggr_dict[i][1] == 'count':
                                                dict_val_list.append((i,
                                                                      ConstantExpr(1)))
                                            else:
                                                raise NotImplementedError
                                        elif isinstance(aggr_dict[i], str):
                                            if aggr_dict[i] == 'sum':
                                                dict_val_list.append((i,
                                                                      self.probe_access(i)))
                                            elif aggr_dict[i] == 'count':
                                                dict_val_list.append((i,
                                                                      ConstantExpr(1)))
                                            else:
                                                raise NotImplementedError
                                        else:
                                            raise ValueError(f'Unexpected aggrgation function: {aggr_dict}')
                                    else:
                                        raise NotImplementedError
                                else:
                                    raise IndexError(f'Cannot find column {i}')

                            dict_val_ir = RecConsExpr(dict_val_list) if dict_val_list else ConstantExpr(True)

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

                            # self.probe_frame.probe_on.transform.migrate(transform)

                            # print(self.joint.name)
                            # print('all', self.joint.columns)
                            # print('used', self.retriever.findall_cols_used())
                            # print('last', last_merge_expr)
                            # print('next', next_merge_expr)
                            # print({
                            #     'key': key_col,
                            #     'vals': val_cols
                            # })
                            # # print(transform)
                            # print(joint_op)
                            # print('==============================')

                            if joint_cond:
                                joint_op = IfExpr(condExpr=joint_cond,
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))

                            non_null_cond = CompareExpr(compareType=CompareSymbol.NE,
                                                        leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                               keyExpr=probe_key_ir),
                                                        rightExpr=ConstantExpr(None))

                            joint_op = IfExpr(condExpr=non_null_cond,
                                              thenBodyExpr=joint_op,
                                              elseBodyExpr=ConstantExpr(None))

                            if probe_isin:
                                joint_op = IfExpr(condExpr=probe_isin.get_as_cond(),
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))

                            if probe_cond:
                                joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                                  thenBodyExpr=joint_op,
                                                  elseBodyExpr=ConstantExpr(None))

                            out = LetExpr(varExpr=probe_var,
                                          valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                                          dictExpr=probe_on.var_expr,
                                                          bodyExpr=joint_op,
                                                          isAssignmentSum=False),
                                          bodyExpr=next_op)

                            if probe_isin:
                                return probe_isin.get_as_part(out)
                            else:
                                return out
                    else:
                        key_col = next_merge_expr.left_on

                    if renamed_cols:
                        if key_col in renamed_cols.keys():
                            key_col = renamed_cols[key_col]

                    dict_key_ir = self.probe_access(key_col)

                    # dict vals: multi(single) column

                    val_cols = [x for x in self.retriever.findall_cols_used()
                                if (x != last_merge_expr.left_on
                                    and x != last_merge_expr.right_on
                                    and x != key_col
                                    and x not in self.retriever.find_renamed_cols())
                                or (x in self.retriever.find_cols_used('merge')
                                    and x != key_col
                                    and x not in self.retriever.find_renamed_cols())
                                or (x in self.retriever.findall_cols_for_groupby_aggr())]

                    if not val_cols:
                        if joint_col_proj:
                            for i in joint_col_proj.proj_cols:
                                val_cols.append(i)

                    dict_val_list = []

                    for i in val_cols:
                        if i == part_key:
                            dict_val_list.append((i,
                                                  probe_key_ir))
                        elif i == probe_key:
                            dict_val_list.append((i,
                                                  probe_key_ir))
                        elif i in self.part_frame.part_on.columns:
                            if i in renamed_cols.keys():
                                dict_val_list.append((renamed_cols[i],
                                                      RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                          keyExpr=probe_on.key_access(
                                                                                              probe_key)),
                                                                    fieldName=renamed_cols[i])))
                            else:
                                dict_val_list.append((i,
                                                      RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                                          keyExpr=probe_on.key_access(
                                                                                              probe_key)),
                                                                    fieldName=i)))
                        elif i in self.probe_frame.probe_on.columns:
                            # print(i, 'in', self.probe_frame.probe_on, 'with', self.probe_frame.probe_on.columns)
                            dict_val_list.append((i,
                                                  probe_on.key_access(i)))

                        elif i in self.retriever.find_renamed_cols(mode='as_val'):
                            origin_col = self.retriever.find_col_rename(col_name=i, by='val')

                            if origin_col in probe_on.columns:
                                dict_val_list.append((i,
                                                      probe_on.key_access(origin_col)))
                            else:
                                dict_val_list.append((i,
                                                      self.part_lookup(origin_col)))

                        elif self.probe_frame.retriever.was_groupby_aggr:
                            groupby_aggr_expr = self.probe_frame.retriever.find_groupby_aggr()
                            groupby_cols = groupby_aggr_expr.groupby_cols
                            aggr_dict = groupby_aggr_expr.origin_dict

                            if i in groupby_cols:
                                dict_val_list.append((i,
                                                      self.probe_access(i)))
                            elif i in aggr_dict.keys():
                                if isinstance(aggr_dict[i], tuple):
                                    if aggr_dict[i][1] == 'sum':
                                        dict_val_list.append((i,
                                                              self.probe_access(aggr_dict[i][0])))
                                    elif aggr_dict[i][1] == 'count':
                                        dict_val_list.append((i,
                                                              ConstantExpr(1)))
                                    else:
                                        raise NotImplementedError
                                elif isinstance(aggr_dict[i], str):
                                    if aggr_dict[i] == 'sum':
                                        dict_val_list.append((i,
                                                              self.probe_access(i)))
                                    elif aggr_dict[i] == 'count':
                                        dict_val_list.append((i,
                                                              ConstantExpr(1)))
                                    else:
                                        raise NotImplementedError
                                else:
                                    raise ValueError(f'Unexpected aggrgation function: {aggr_dict}')
                            else:
                                raise NotImplementedError
                        else:
                            raise IndexError(f'Cannot find column {i}')

                    dict_val_ir = RecConsExpr(dict_val_list) if dict_val_list else ConstantExpr(True)

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
                                          elseBodyExpr=ConstantExpr(None))

                    non_null_cond = CompareExpr(compareType=CompareSymbol.NE,
                                                leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                       keyExpr=probe_key_ir),
                                                rightExpr=ConstantExpr(None))

                    joint_op = IfExpr(condExpr=non_null_cond,
                                      thenBodyExpr=joint_op,
                                      elseBodyExpr=ConstantExpr(None))

                    if probe_isin:
                        joint_op = IfExpr(condExpr=probe_isin.get_as_cond(),
                                          thenBodyExpr=joint_op,
                                          elseBodyExpr=ConstantExpr(None))

                    if probe_cond:
                        joint_op = IfExpr(condExpr=probe_cond.sdql_ir,
                                          thenBodyExpr=joint_op,
                                          elseBodyExpr=ConstantExpr(None))

                    out = LetExpr(varExpr=probe_var,
                                  valExpr=SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                                                  dictExpr=probe_on.var_expr,
                                                  bodyExpr=joint_op,
                                                  isAssignmentSum=False),
                                  bodyExpr=next_op)

                    if probe_isin:
                        return probe_isin.get_as_part(out)
                    else:
                        return out

            # Q5
            # Q10
            if self.retriever.as_probe_for_next_join:
                return next_op

    def get_joint_expr(self, next_op=None):
        # Q14
        # Q15
        # Q16
        # Q17
        # Q18
        # Q19
        if not self.part_frame.is_joint and not self.probe_frame.is_joint:
            print(f'{self.joint.name}: neither joint')

            if self.probe_frame.retriever.was_groupby_aggr:
                if self.probe_frame.retriever.find_cond_after(GroupbyAggrExpr):
                    # Q15
                    # Q18

                    # groupby having (condition after groupby aggregation)

                    result = SDQLInspector.concat_bindings([self.probe_frame.probe_on.get_groupby_aggr(),
                                                            self.part_frame.get_part_expr(),
                                                            self.get_probe_expr(next_op)])
                    return result
                else:
                    # Q17
                    return self.part_frame.get_part_expr(self.get_probe_expr(next_op))

            # Q14
            # Q16
            # Q19

            return self.part_frame.get_part_expr(self.get_probe_expr(next_op))
        # Q10
        # Q18
        if self.part_frame.is_joint and not self.probe_frame.is_joint:
            print(f'{self.joint.name}: part joint')

            return self.part_frame.part_on.get_joint_frame().get_joint_expr(self.get_probe_expr(next_op))
        # Q10
        if not self.part_frame.is_joint and self.probe_frame.is_joint:
            print(f'{self.joint.name}: probe joint')

            if self.part_frame.retriever.as_bypass_for_next_join:
                return self.probe_frame.probe_on.retriever.find_merge(
                    mode='as_joint').joint.get_joint_frame().get_joint_expr(
                    self.part_frame.get_part_expr(self.get_probe_expr(next_op)))

            # Q10

            all_bindings = []

            all_part_expr = self.retriever.findall_part_for_root_probe('as_expr')

            all_bindings += all_part_expr

            all_bindings.append(self.get_probe_expr(next_op))

            return SDQLInspector.concat_bindings(all_bindings)
        if self.part_frame.is_joint and self.probe_frame.is_joint:
            print(f'{self.joint.name}: both joint')

            # if self.retriever.find_illegal_dup_col():
            #     raise ValueError(f'Detected duplicated columns in merge: {self.retriever.find_illegal_dup_col()}')

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

            # Q2

            all_bindings = []

            all_part_expr = self.retriever.findall_part_for_root_probe('as_expr')

            all_bindings += all_part_expr
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
