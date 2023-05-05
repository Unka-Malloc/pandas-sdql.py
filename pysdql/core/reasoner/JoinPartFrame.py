from pysdql.core.interfaces.availability.Replaceable import Replaceable
from pysdql.core.killer.SDQLInspector import SDQLInspector
from pysdql.core.prototype.basic.sdql_ir import (
    Expr,
    IfExpr,
    DicConsExpr,
    RecConsExpr,
    SumExpr,
    LetExpr,
    ConstantExpr,
    RecAccessExpr
)
from pysdql.core.killer.Retriever import Retriever


class JoinPartFrame:
    def __init__(self, iter_on, col_proj):
        self.__group_key = None
        self.__iter_cond = None
        self.__col_proj = col_proj if col_proj else []
        self.__iter_on = iter_on
        self.__var_partition = iter_on.get_var_part()
        self.__next_probe = None

    def get_part_col_proj(self):
        if self.__col_proj:
            return self.__col_proj

    @property
    def retriever(self) -> Retriever:
        return self.get_part_on().get_retriever()

    @property
    def next_probe(self):
        return self.__next_probe

    @property
    def part_on_iter_el(self):
        return self.part_on.iter_el.sdql_ir

    @property
    def group_key(self):
        renamed_cols = self.retriever.findall_col_rename(reverse=True)
        if isinstance(self.__group_key, list) and len(self.__group_key) == 1:
            self.__group_key = self.__group_key[0]

        if isinstance(self.__group_key, str):
            if self.__group_key in renamed_cols.keys():
                return renamed_cols[self.__group_key]

        return self.__group_key

    @property
    def part_key(self):
        if isinstance(self.__group_key, list) and len(self.__group_key) == 1:
            self.__group_key = self.__group_key[0]

        renamed_cols = self.retriever.findall_col_rename(reverse=True)
        if isinstance(self.__group_key, str):
            if self.__group_key in renamed_cols.keys():
                return renamed_cols[self.__group_key]

        return self.__group_key

    @property
    def partition_on(self):
        return self.__iter_on

    @property
    def part_on(self):
        return self.__iter_on

    def get_part_var(self):
        return self.__var_partition

    @property
    def part_var(self):
        return self.__var_partition

    @property
    def part_on_var(self):
        return self.__iter_on.var_part

    @property
    def cols_out(self):
        return self.__iter_on.cols_out

    def get_part_on(self):
        return self.__iter_on

    def get_partition_on(self):
        return self.__iter_on

    def get_part_dict_key(self) -> list:
        if isinstance(self.part_key, str):
            return [self.part_key]
        elif isinstance(self.part_key, list):
            return self.part_key

    def get_part_dict_val(self) -> list:
        col_proj = self.retriever.find_col_proj().proj_cols
        col_proj = [col for col in col_proj if self.group_key != col]

        cols_out = self.retriever.findall_cols_used(only_next=True)
        cols_out += self.retriever.find_cols_probed()

        if col_proj:
            return col_proj
        else:
            return cols_out

    @property
    def col_proj_ir(self):
        col_proj = self.retriever.find_col_proj()
        col_agg_dict = self.retriever.find_groupby_aggr()

        if col_proj:
            col_proj = col_proj.proj_cols

            if len(col_proj) == 0:
                return ConstantExpr(True)
            else:
                cols_out = []
                for i in col_proj:
                    if col_agg_dict:
                        if i in col_agg_dict.origin_dict.keys():
                            if col_agg_dict.origin_dict[i][1] == 'sum':
                                cols_out.append((i, self.part_on.key_access(col_agg_dict.origin_dict[i][0])))
                            elif col_agg_dict.origin_dict[i][1] == 'count':
                                cols_out.append((i, ConstantExpr(1)))
                            else:
                                cols_out.append((i, self.part_on.key_access(col_agg_dict.origin_dict[i][0])))
                    else:
                        cols_out.append((i, self.part_on.key_access(i)))

                return RecConsExpr(cols_out)
        else:
            if self.retriever.as_bypass_for_next_join:
                return ConstantExpr(True)
            else:
                cols_out = self.retriever.findall_cols_used(only_next=True)

                cols_out += self.retriever.find_cols_probed()

                if len(cols_out) == 0:
                    return ConstantExpr(True)
                else:
                    out_list = []

                    cols_renamed = self.retriever.findall_col_rename()
                    for i in cols_out:
                        if i in cols_renamed.keys():
                            out_list.append((cols_renamed[i], self.part_on.key_access(i)))
                        elif i in cols_renamed.values():
                            continue
                        else:
                            out_list.append((i, self.part_on.key_access(i)))

                    return RecConsExpr(out_list)

    def add_key(self, val):
        self.__group_key = val

    def add_cond(self, val):
        self.__iter_cond = val

    def add_col_proj(self, val):
        self.__col_proj = val

    def add_probe(self, val):
        self.__next_probe = val

    @property
    def is_joint(self):
        return self.partition_on.is_joint

    def get_part_key(self):
        return self.__group_key

    def get_part_expr(self, next_probe_op=None):
        # print(self.part_on.get_as_build_end())

        if not next_probe_op:
            if self.next_probe:
                next_probe_op = self.next_probe
            else:
                next_probe_op = ConstantExpr(True)

        if isinstance(self.part_key, str):
            if self.retriever.as_aggr_for_next_join:
                next_merge = self.retriever.find_merge('as_part')
                groupby_aggr_expr = next_merge.joint.retriever.findall_groupby_aggr()[0]

                groupby_cols = groupby_aggr_expr.groupby_cols
                aggr_dict = groupby_aggr_expr.aggr_dict

                if len(groupby_cols) == 0:
                    raise ValueError()
                elif len(groupby_cols) == 1:
                    dict_key_ir = self.part_on.key_access(self.part_key)
                else:
                    key_tuples = []
                    for c in groupby_cols:
                        if c == next_merge.right_on:
                            key_tuples.append((c, self.part_on.key_access(self.part_key)))
                        elif c in self.part_on.columns:
                            key_tuples.append((c, self.part_on.key_access(c)))
                        else:
                            raise IndexError(f'Cannot find such a column {c} '
                                             f'in part side {self.part_on.name}')
                    dict_key_ir = RecConsExpr(key_tuples)

                val_tuples = []
                for k in aggr_dict.keys():
                    v = aggr_dict[k]

                    if isinstance(v, RecAccessExpr):
                        # (, 'sum')
                        if v.name in self.part_on.columns:
                            val_tuples.append((k, self.part_on.key_access(v.name)))
                        else:
                            raise IndexError(f'Cannot find column {v.name} in {self.part_on.columns}')
                    elif isinstance(v, ConstantExpr):
                        # (, 'count')
                        val_tuples.append((k, v))
                    else:
                        raise NotImplementedError
                dict_val_ir = RecConsExpr(val_tuples)

                part_left_op = DicConsExpr([(dict_key_ir, dict_val_ir)])
            else:
                part_left_op = DicConsExpr([(
                    self.part_on.key_access(self.part_key),
                    self.col_proj_ir)])

            isin_expr = self.retriever.find_isin('as_probe')

            if isin_expr:
                part_left_op = IfExpr(condExpr=isin_expr.get_as_cond(),
                                      thenBodyExpr=part_left_op,
                                      elseBodyExpr=ConstantExpr(None))

            if self.part_cond:
                part_left_op = IfExpr(condExpr=self.part_cond,
                                      thenBodyExpr=part_left_op,
                                      elseBodyExpr=ConstantExpr(None))

            part_left_sum = SumExpr(varExpr=self.part_on_iter_el,
                                    dictExpr=self.part_on.var_expr,
                                    bodyExpr=part_left_op,
                                    isAssignmentSum=True)

            part_left_let = LetExpr(varExpr=self.part_var,
                                    valExpr=part_left_sum,
                                    bodyExpr=next_probe_op)

            if isin_expr:
                return isin_expr.get_as_part(part_left_let)
            else:
                return part_left_let
        elif isinstance(self.part_key, list):
            all_isin_expr = self.retriever.findall_isin()
            all_isin_cond = []
            all_isin_part = []

            if all_isin_expr:
                for e in all_isin_expr:
                    all_isin_cond.append(e.get_as_cond())
                    all_isin_part.append(e.get_as_part())

            if self.retriever.as_bypass_for_next_join:
                part_left_op = DicConsExpr([(RecConsExpr([(k, self.part_on.key_access(k)) for k in self.group_key]),
                                             ConstantExpr(True))])

                if self.part_cond:
                    part_left_op = IfExpr(condExpr=self.part_cond,
                                          thenBodyExpr=part_left_op,
                                          elseBodyExpr=ConstantExpr(None))

                part_left_sum = SumExpr(varExpr=self.part_on_iter_el,
                                        dictExpr=self.__iter_on.var_expr,
                                        bodyExpr=part_left_op,
                                        isAssignmentSum=True)

                part_left_let = LetExpr(varExpr=self.part_var,
                                        valExpr=part_left_sum,
                                        bodyExpr=next_probe_op)

                return part_left_let
            elif self.retriever.was_groupby_aggr:
                groupby_aggr_expr = self.retriever.find_groupby_aggr()

                groupby_cols = groupby_aggr_expr.groupby_cols
                aggr_dict = groupby_aggr_expr.aggr_dict

                if len(groupby_cols) == 0:
                    raise ValueError()
                elif len(groupby_cols) == 1:
                    dict_key_ir = self.part_on.key_access(groupby_cols[0])
                else:
                    key_tuples = []
                    for c in groupby_cols:
                        if c in self.part_on.columns:
                            key_tuples.append((c, self.part_on.key_access(c)))
                        else:
                            raise IndexError(f'Cannot find such a column {c} '
                                             f'in part side {self.part_on.name}')
                    dict_key_ir = RecConsExpr(key_tuples)

                val_tuples = []
                for k in aggr_dict.keys():
                    v = aggr_dict[k]

                    if isinstance(v, RecAccessExpr):
                        # (, 'sum')
                        if v.name in self.part_on.columns:
                            val_tuples.append((k, self.part_on.key_access(v.name)))
                        else:
                            raise IndexError(f'Cannot find column {v.name} in {self.part_on.columns}')
                    elif isinstance(v, ConstantExpr):
                        # (, 'count')
                        val_tuples.append((k, v))
                    else:
                        raise NotImplementedError
                dict_val_ir = RecConsExpr(val_tuples)

                part_left_op = DicConsExpr([(dict_key_ir, dict_val_ir)])

                if all_isin_expr:
                    for cond in all_isin_cond:
                        part_left_op = IfExpr(condExpr=cond,
                                              thenBodyExpr=part_left_op,
                                              elseBodyExpr=ConstantExpr(None))
            else:
                part_left_op = DicConsExpr([(
                    RecConsExpr([(k, self.part_on.key_access(k)) for k in self.group_key]),
                    self.col_proj_ir)])

            if self.part_cond:
                part_left_op = IfExpr(condExpr=self.part_cond,
                                      thenBodyExpr=part_left_op,
                                      elseBodyExpr=ConstantExpr(None))

            part_left_sum = SumExpr(varExpr=self.part_on_iter_el,
                                    dictExpr=self.part_on.var_expr,
                                    bodyExpr=part_left_op,
                                    isAssignmentSum=True)

            part_left_let = LetExpr(varExpr=self.part_var,
                                    valExpr=part_left_sum,
                                    bodyExpr=next_probe_op)

            all_bindings = []

            all_bindings += all_isin_part
            all_bindings.append(part_left_let)

            return SDQLInspector.concat_bindings(all_bindings)
        else:
            raise NotImplementedError

    @property
    def part_cond(self):
        cond = self.retriever.find_cond()

        if isinstance(cond, Expr):
            return cond
        if isinstance(cond, Replaceable):
            return cond.sdql_ir
        else:
            Warning(f'NOT safe condition {type(cond)} : {cond} at partition side')
            return cond

    @property
    def filled(self):
        if self.__group_key:
            return True
        return False

    @property
    def finished(self):
        if self.__group_key and self.__next_probe:
            return True
        return False

    @property
    def sdql_ir(self):
        if self.partition_on.is_joint:
            return self.partition_on.get_joint_frame().sdql_ir

    def __repr__(self):
        if self.partition_on.is_joint:
            joint_frame = self.partition_on.get_joint_frame()
            return repr(joint_frame)

        return str(
            {
                'patition': 'frame',
                'part_key': self.__group_key,
                'cond': self.__iter_cond,
                'cols': self.__col_proj,
                'var': self.__var_partition
            }
        )
    