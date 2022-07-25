import os
import string

from pysdql.core.dtypes.ConcatExpr import ConcatExpr
from pysdql.core.dtypes.ArrayExpr import ArrayExpr
from pysdql.core.dtypes.CaseExpr import CaseExpr
from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.ExistExpr import ExistExpr
from pysdql.core.dtypes.IsinExpr import IsinExpr
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.CondStmt import CondStmt
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.DictExpr import DictExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.SDict import sdict
from pysdql.core.dtypes.SetExpr import SetExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.GroupbyExpr import GroupbyExpr
from pysdql.core.dtypes.LoadExpr import LoadExpr
from pysdql.core.dtypes.ExternalExpr import ExternalExpr


class relation:
    def __init__(self, name, data=None, cols=None, inherit_from=None, operations=None, promoted_cols=None):
        """
        If you need an optimizer, give it to relation and pass to all classes
        :param name:
        :param cols:
        :param compo_expr:
        :param inherit_from:
        """

        if cols is None:
            cols = []

        self.name = name
        self.cols = cols
        self.data = data

        self.history_name = []
        self.operations = []

        if operations:
            self.operations += operations

        if self.data:
            if type(data) == sdict:
                self.operations.append(OpExpr('relation_data', VarExpr(self.name, self.data)))
            if type(data) == list:
                self.operations.append(OpExpr('relation_merge_data', VarExpr(self.name, self.data)))
            if type(data) == LoadExpr:
                self.operations.append(OpExpr('relation_load_data', VarExpr(self.name, self.data)))

        if inherit_from:
            self.inherit(inherit_from)

        self.using_col = []

        if promoted_cols is None:
            self.promoted_cols = {}
        else:
            self.promoted_cols = promoted_cols

    @property
    def iter_expr(self):
        return IterExpr(self.name)

    @property
    def key(self):
        return self.iter_expr.key

    @property
    def val(self):
        return self.iter_expr.val

    @property
    def last_operation(self):
        if self.operations:
            return self.operations[-1]
        return ''

    def rename(self, name):
        tmp_var = VarExpr(name, self.name)
        self.history_name.append(name)
        self.operations.append(OpExpr('relation_rename', tmp_var))

        return relation(name=name,
                        cols=self.cols,
                        inherit_from=self,
                        promoted_cols=self.promoted_cols)

    def gen_tmp_name(self, noname=None):
        if noname is None:
            noname = []
        name_list = [f'{str(self.name[0]).lower()}mp', 'tmp']
        for i in list(string.ascii_lowercase):
            name_list.append(f'tmp{i}')

        dup_list = [self.name] + self.history_name + noname

        for tmp_name in name_list:
            if tmp_name not in dup_list:
                return tmp_name

    def selection(self, item: CondExpr):
        """let rmp = if (is_empty) then Rm0 else sum (<r0_k, r0_v> in Rm0) sum (<ps_k, ps_v> in part_s) if ((!((r0_k.ps_suppkey == ps_k.s_suppkey)))) then { r0_k -> 1 } else {  } """
        if item.isin:
            if item.inherit_from:
                self.inherit(item.inherit_from)

            cond_expr = CondStmt(conditions=item,
                                 then_case=DictExpr({self.iter_expr.key: 1}),
                                 else_case=DictExpr({}))

            iter_stmt = IterStmt(iter_expr=[self.iter_expr, item.inherit_from.iter_expr],
                                 any_expr=cond_expr)
            if item.op == '~':
                check_empty = CondStmt(conditions=CondExpr(f'{item.inherit_from}', '==', '{ }'),
                                       then_case=self.name,
                                       else_case=iter_stmt)
                var_name = self.gen_tmp_name()

                self.history_name.append(var_name)
                self.operations.append(OpExpr('relation_selection', VarExpr(var_name, check_empty)))
        else:
            if item.inherit_from:
                self.inherit(item.inherit_from)
            item = item.new_cond(self.iter_expr.key)
            cond_expr = CondStmt(conditions=item,
                                 then_case=DictExpr({self.iter_expr.key: 1}),
                                 else_case=DictExpr({}),
                                 new_iter=self.iter_expr.key)
            iter_stmt = IterStmt(iter_expr=self.iter_expr,
                                 any_expr=cond_expr)

        var_name = self.gen_tmp_name()

        self.history_name.append(var_name)
        self.operations.append(OpExpr('relation_selection', VarExpr(var_name, iter_stmt)))

        return relation(name=var_name,
                        cols=self.cols,
                        inherit_from=self)

    def projection(self, cols):
        tmp_dict = {}
        for i in cols:
            tmp_dict[i] = f'{self.iter_expr.key}.{i}'

        compo_expr = IterStmt(iter_expr=self.iter_expr,
                              any_expr=DictExpr({RecExpr(tmp_dict): 1}))

        next_name = self.gen_tmp_name()

        self.operations.append(OpExpr('relation_projection', VarExpr(next_name, compo_expr)))

        result = relation(name=next_name,
                          inherit_from=self)
        return result

    def get_col(self, col_name):
        if col_name in self.promoted_cols.keys():
            return ColEl(self, col_name, promoted=self.promoted_cols[col_name])
        else:
            return ColEl(self, col_name)

    def selection_isin(self, isin_expr: IsinExpr):
        unit1 = isin_expr.unit1
        unit2 = isin_expr.unit2

        r2 = unit2.relation
        self.inherit(r2)

        cond_unit = CondExpr(unit1, '==', unit2)
        cond_expr = CondStmt(conditions=cond_unit,
                             then_case=DictExpr({self.iter_expr.key: 1}),
                             else_case=DictExpr({}))
        compo_expr = IterStmt(iter_expr=[self.iter_expr, r2.iter_expr],
                              any_expr=cond_expr)

        var_name = self.gen_tmp_name()

        result = relation(name=var_name,
                          inherit_from=self)

        self.operations.append(OpExpr('relation_selection_isin', VarExpr(var_name, compo_expr)))
        return result

    def __getattr__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

    def selection_external(self, item):
        cond_expr = CondStmt(conditions=item.new_expr(self.iter_expr.key),
                             then_case=DictExpr({self.iter_expr.key: 1}),
                             else_case=DictExpr({}))
        compo_expr = IterStmt(iter_expr=self.iter_expr,
                              any_expr=cond_expr)

        var_name = self.gen_tmp_name()

        self.history_name.append(var_name)
        self.operations.append(OpExpr('relation_selection_ext', VarExpr(var_name, compo_expr)))

        return relation(name=var_name,
                        cols=self.cols,
                        inherit_from=self)

    def selection_exists(self, item):
        main_col = item.main_col
        sub_col = item.sub_col
        main_r = item.main_r
        sub_r = item.sub_r

        if item.conds:
            tmp_cond = (main_col == sub_col) & item.conds
        else:
            tmp_cond = (main_col == sub_col)

        if item.flag == 'not_exists':
            exists_name = f'exists_{sub_r.name}'
            step1 = VarExpr(exists_name, IterStmt([main_r.iter_expr, sub_r.iter_expr],
                                                  CondStmt(conditions=tmp_cond,
                                                           then_case=DictExpr(
                                                               {RecExpr({main_col.name: main_col.expr}): 1}),
                                                           else_case=DictExpr(
                                                               {RecExpr({main_col.name: main_col.expr}): 0}))))
            self.history_name.append(exists_name)
            self.operations.append(OpExpr('relation_selection_not_exists_groupby', step1))

            next_name = self.gen_tmp_name(sub_r.history_name)
            tmp_rec = RecExpr({main_col.name: f'{self.iter_expr.key}.{main_col.name}'})
            step2 = VarExpr(next_name,
                            IterStmt(self.iter_expr,
                                     CondStmt(CondExpr(f'{exists_name}({tmp_rec})', '==', 0),
                                              DictExpr({self.iter_expr.key: 1}), DictExpr({})))
                            )
            self.history_name.append(next_name)
            self.operations.append(OpExpr('relation_selection_not_exists_output', step2))

            next_relation = relation(name=next_name,
                                     inherit_from=self)
            next_relation.inherit(sub_r)

            return next_relation
        else:
            exists_name = f'exists_{sub_r.name}'
            step1 = VarExpr(exists_name, IterStmt([main_r.iter_expr, sub_r.iter_expr],
                                                  CondStmt(conditions=tmp_cond,
                                                           then_case=DictExpr(
                                                               {RecExpr({main_col.name: main_col.expr}): 1}),
                                                           else_case=DictExpr(
                                                               {RecExpr({main_col.name: main_col.expr}): 0}))))
            self.history_name.append(exists_name)
            self.operations.append(OpExpr('relation_selection_exists_groupby', step1))

            next_name = self.gen_tmp_name(sub_r.history_name)
            tmp_rec = RecExpr({main_col.name: f'{self.iter_expr.key}.{main_col.name}'})
            step2 = VarExpr(next_name,
                            IterStmt(self.iter_expr,
                                     CondStmt(CondExpr(f'{exists_name}({tmp_rec})', '>', 0), DictExpr({self.iter_expr.key: 1}), DictExpr({})))
                            )
            self.history_name.append(next_name)
            self.operations.append(OpExpr('relation_selection_exists_output', step2))

            next_relation = relation(name=next_name,
                                     inherit_from=self)
            next_relation.inherit(sub_r)

            return next_relation

        # cond_expr = CondStmt(conditions=item,
        #                      then_case=DictExpr({self.iter_expr.key: 1}),
        #                      else_case=DictExpr({}))
        # compo_expr = IterStmt(iter_expr=self.iter_expr,
        #                       any_expr=cond_expr)
        #
        # var_name = self.gen_tmp_name()
        #
        # self.history_name.append(var_name)
        # self.operations.append(OpExpr('relation_selection_ext', VarExpr(var_name, compo_expr)))

        # return relation(name=var_name,
        #                 cols=self.cols,
        #                 inherit_from=self)

    def __getitem__(self, item):
        if type(item) == CondExpr:
            return self.selection(item)
        if type(item) == str:
            return self.get_col(col_name=item)
        if type(item) == list:
            return self.projection(item)
        if type(item) == IsinExpr:
            return self.selection_isin(item)
        if type(item) == ExternalExpr:
            return self.selection_external(item)
        if type(item) == ExistExpr:
            return self.selection_exists(item)
        print(item)

    def __setitem__(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        if type(key) == list and type(value) == list:
            return self.insert_col_list(key, value)

        if type(value) in (ColEl, ColExpr, CaseExpr, ExternalExpr):
            return self.insert_col(key, value)

    def insert_col(self, key, value):
        if type(value) == CaseExpr:
            tmp_name = self.gen_tmp_name()

            self.cols.append(key)

            output = value.set(key, tmp_name, self.iter_expr)
            self.history_name.append(tmp_name)
            self.operations.append(OpExpr('relation_set_caseexpr', output))

            self.name = tmp_name
            return self
        if type(value) == ColEl or type(value) == ColExpr:
            return self.col_rename(from_col=value, to_col=ColEl(relation=self, col_name=key))
        if type(value) == ExternalExpr:
            return self.col_rename(from_col=value, to_col=ColEl(relation=self, col_name=key))

    def insert_col_list(self, key, value):
        if len(key) != len(value):
            raise ValueError()

        next_name = self.gen_tmp_name()

        compo = IterStmt(self.iter_expr,
                         DictExpr({f'concat({self.iter_expr.key}, {RecExpr(dict(zip(key, value)))})': 1}))

        output = VarExpr(next_name, compo)

        self.name = next_name
        self.history_name.append(next_name)
        self.operations.append(OpExpr('relation_setitem_list_loopfusion', output))

    def keep_cols(self):
        tmp_dict = {}
        for i in self.cols:
            tmp_dict[i] = ColEl(relation=self, col_name=i)
        return tmp_dict

    def col_rename(self, from_col, to_col):
        """

        :param from_col: ColExpr
        :param to_col: ColUnit
        :return:
        """

        # print(f'rename column from {from_col} to {to_col}')

        next_name = self.gen_tmp_name()

        tmp_concat_expr = ConcatExpr(self.iter_expr.key, RecExpr({to_col.name: from_col.new_expr(self.iter_expr.key)}))
        tmp_dict_expr = DictExpr({tmp_concat_expr: 1})

        output = VarExpr(next_name, IterStmt(self.iter_expr, tmp_dict_expr))

        self.name = next_name
        self.history_name.append(next_name)
        self.operations.append(OpExpr('relation_rename_col', output))

        return self

    @property
    def expr(self) -> str:
        return f'{self.name}'

    def __repr__(self):
        return self.expr

    def show(self):
        print(self.sdql_expr)

    def from_cols(self, col_list: list):
        """
        Create a new relation with column list ['col1', 'col2', ..., 'coln']
        :param col_list:
        :return:
        """
        tmp_dict = {}
        for col in col_list:
            tmp_dict[col] = f'{self.iter_expr.key}.{col}'

        return relation(name=self.gen_tmp_name(),
                        cols=self.cols,
                        inherit_from=self
                        )

    def groupby(self, cols):
        if type(cols) != list:
            raise TypeError()
        """
        let tmp = sum (<l_k, l_v> in lineitem) { <l_returnflag=l_k.l_returnflag, l_linestatus=l_k.l_linestatus> -> {l_k -> l_v} }
        in let tmpa = sum(<t_k, t_v> in tmp) { <l_returnflag=t_k.l_returnflag, l_linestatus=t_k.l_linestatus, group=t_v> }
        """
        var_name = self.gen_tmp_name()
        return GroupbyExpr(name=var_name,
                           groupby_from=self,
                           groupby_cols=cols)

    def aggregate(self, aggr_func=None, *aggr_args, **aggr_kwargs):
        if aggr_func:
            if type(aggr_func) == dict:
                return self.agg_dict_parse(aggr_func)
        if aggr_kwargs:
            return self.agg_kwargs_parse(aggr_kwargs)

    def agg(self, aggr_func=None, *aggr_args, **aggr_kwargs):
        """
        Aggregation
        :param aggr_func:
        :param args:
        :param kwargs:
        :return:
        """
        if aggr_func:
            if type(aggr_func) == dict:
                return self.agg_dict_parse(aggr_func)
        if aggr_kwargs:
            return self.agg_kwargs_parse(aggr_kwargs)

    def agg_dict_parse(self, aggr_dict: dict):
        """
        This function produce an Array of values.
        :param aggr_dict:
        :return:
        """
        # new_name = self.gen_tmp_name()
        #
        # tmp_dict = {}
        # avg_n = 1
        # for aggr_key in aggr_func.keys():
        #     aggr_val = aggr_func[aggr_key]
        #     aggr_calc = aggr_key
        #     if type(aggr_calc) == ColEl or type(aggr_calc) == ColExpr:
        #         aggr_calc = aggr_calc.new_expr(f'{self.iter_expr.key}')
        #     if aggr_val == 'sum':
        #         tmp_dict[aggr_key] = f'{aggr_calc} * {self.iter_expr.val}'
        #     if aggr_val == 'count':
        #         tmp_dict[aggr_key] = f'{self.iter_expr.val}'
        #     if aggr_val == 'avg':
        #         print(f'let sc_{avg_n} = <s=({aggr_calc} * {self.iter_expr.val}), c={self.iter_expr.val}>')
        #         tmp_dict[aggr_key] = f'(sc_{avg_n}.s / sc_{avg_n}.c)'
        #         avg_n += 1
        #
        # aggr_array = ', '.join(tmp_dict.values())
        #
        # print(f'let {new_name} = {self.iter_expr} [| {aggr_array} |]')
        #
        # return ArrayExpr(new_name, list(tmp_dict.values()))

        next_name = f'agg_value'
        next_relation = relation(name=next_name,
                                 inherit_from=self)

        # print(next_relation.operations, aggr_dict)

        tmp_dict = {}
        result_dict = {}

        aggr_tuple_name = self.gen_tmp_name()
        aggr_tuple_iter_expr = IterExpr(aggr_tuple_name)

        result_str = ''

        for aggr_key in aggr_dict.keys():
            dict_val = aggr_dict[aggr_key]
            aggr_prefix = 'the'
            if type(aggr_key) == ColEl:
                aggr_prefix = f'{aggr_key.name}'
            if dict_val == 'sum':
                tmp_dict[f'{aggr_prefix}_sum'] = f'{aggr_key} * {self.iter_expr.val}'
                result_dict[f'{aggr_prefix}_sum'] = f'{aggr_tuple_iter_expr.val}.{aggr_prefix}_sum'
                result_str = f'{aggr_tuple_name}.{aggr_prefix}_sum'
            if dict_val == 'count':
                tmp_dict[f'{aggr_prefix}_count'] = f'{self.iter_expr.val}'
                result_dict[f'{aggr_prefix}_count'] = f'{aggr_tuple_iter_expr.val}.{aggr_prefix}_count'
                result_str = f'{aggr_tuple_name}.{aggr_prefix}_count'
            if dict_val == 'avg':
                tmp_dict[f'{aggr_prefix}_sum'] = f'{self.iter_expr.key}.{aggr_prefix} * {self.iter_expr.val}'
                tmp_dict[f'{aggr_prefix}_count'] = f'{self.iter_expr.val}'
                result_dict[f'{aggr_prefix}_avg'] = f'{aggr_tuple_iter_expr.val}.{aggr_prefix}_sum' \
                                                    f' / ' \
                                                    f'{aggr_tuple_iter_expr.val}.{aggr_prefix}_count'
                result_str = f'{aggr_tuple_name}.{aggr_prefix}_sum' \
                             f' / ' \
                             f'{aggr_tuple_name}.{aggr_prefix}_count'
            if dict_val == 'min':
                pass
            if dict_val == 'max':
                tmp_dict[f'{aggr_prefix}_max'] = f'promote[mxpr]({self.iter_expr.key}.{aggr_prefix})'
                result_dict[f'{aggr_prefix}_max'] = f'{aggr_tuple_iter_expr.val}.{aggr_prefix}_max'
                result_str = f'{aggr_tuple_name}.{aggr_prefix}_max'

        agg_tuple = VarExpr(name=aggr_tuple_name,
                            data=IterStmt(self.iter_expr, RecExpr(tmp_dict)))
        next_relation.history_name.append(aggr_tuple_name)
        next_relation.operations.append(OpExpr('relation_agg_dict_agg_tuple', agg_tuple))

        result = VarExpr(name=next_name,
                         data=result_str)
        next_relation.history_name.append(next_name)
        next_relation.operations.append(OpExpr('relation_agg_dict_agg_result_value', result))

        return next_relation

        # output_cols = list(aggr_dict.keys())
        # return relation(name=next_name,
        #                 cols=output_cols,
        #                 inherit_from=self)

    def agg_kwargs_parse(self, aggr_dict: dict):
        result_dict = {}

        aggr_record = self.gen_tmp_name()

        tmp_name = self.gen_tmp_name()
        tmp_iter_expr = IterExpr(tmp_name)

        tmp_dict = {}
        for aggr_key in aggr_dict.keys():
            aggr_val = aggr_dict[aggr_key]
            if type(aggr_val) == ColEl:
                result_dict[aggr_key] = f'{aggr_record}.{aggr_dict[aggr_key].name}'
            if type(aggr_val) == tuple:
                if type(aggr_dict[aggr_key][0]) == ColExpr and aggr_dict[aggr_key][1] == 'avg':
                    print(f'You should not use {aggr_dict[aggr_key][0]} with "avg" aggregation')
                if type(aggr_dict[aggr_key][0]) == ColEl or type(aggr_dict[aggr_key][0]) == ColExpr:
                    aggr_calc = aggr_dict[aggr_key][0].new_expr(f'{self.iter_expr.key}')
                else:
                    aggr_calc = aggr_dict[aggr_key][0]
                aggr_flag = aggr_dict[aggr_key][1]
                if aggr_flag == 'sum':
                    tmp_dict[aggr_key] = f'{aggr_calc} * {self.iter_expr.val}'
                    result_dict[aggr_key] = f'{aggr_record}.{aggr_key}'
                if aggr_flag == 'count':
                    tmp_dict[aggr_key] = f'{self.iter_expr.val}'
                    result_dict[aggr_key] = f'{aggr_record}.{aggr_key}'
                if aggr_flag == 'avg':
                    tmp_dict[f'{aggr_key}_sum'] = f'{aggr_calc} * {self.iter_expr.val}'
                    tmp_dict[f'{aggr_key}_count'] = f'{self.iter_expr.val}'
                    result_dict[aggr_key] = f'({aggr_record}.{aggr_key}_sum' \
                                            f' / ' \
                                            f'{aggr_record}.{aggr_key}_count)'

        aggr_record = VarExpr(tmp_name, IterStmt(self.iter_expr, RecExpr(tmp_dict)))
        self.history_name.append(tmp_name)
        self.operations.append(OpExpr('relation_aggr_kwargs_aggr_record', aggr_record))

        new_name = self.gen_tmp_name()

        aggr_result = VarExpr(new_name, DictExpr({RecExpr(result_dict): 1}))
        self.history_name.append(new_name)
        self.operations.append(OpExpr('relation_aggr_kwargs_aggr_result', aggr_result))

        return relation(name=new_name,
                        inherit_from=self)

    def aggr_on_col(self, col_name: str, aggr_func=None, *args, **kwargs):
        """
        This function should be ONLY called by ColUnit.aggr()
        :param col_name:
        :param aggr_func:
        :param args:
        :param kwargs:
        :return: pysdql.Relation
        """
        new_r = self.from_cols(col_list=[col_name])
        print(new_r)
        return new_r.agg(aggr_func, *args, **kwargs)

    def access(self):
        return self

    def gen_merged_name(self):
        dup_list = [self.name] + self.history_name

        protoname = 'Rm'

        for i in range(100):
            output = f'{protoname}{i}'
            if output not in dup_list:
                return output

    def merge(self, right, on=None, left_on=None, right_on=None, how='inner', name=None):
        if not type(right) == relation:
            raise TypeError()

        if on is None:
            if left_on and right_on:
                optimized = True
            else:
                raise ValueError()
        else:
            if left_on is None and right_on is None:
                optimized = False
            else:
                raise ValueError()

        if optimized:
            return self.optimized_merge(right, left_on, right_on)

        if name:
            merged_name = name
        else:
            merged_name = self.gen_merged_name()
        if on:
            result = VarExpr(merged_name, IterStmt([self.iter_expr, right.iter_expr],
                                                   CondStmt(on, DictExpr(
                                                       {f'concat({self.iter_expr.key}, {right.iter_expr.key})':
                                                            f'{self.iter_expr.val} * {right.iter_expr.val}'}),
                                                            DictExpr({}))))
            self.history_name.append(merged_name)
            self.operations.append(OpExpr('relation_merge_on_by_concatexpr', result))

            new_cols = self.cols + right.cols
            return relation(name=merged_name, cols=new_cols, inherit_from=self).inherit(right)
        elif left_on and right_on:
            pass
        else:
            result = VarExpr(merged_name, IterStmt([self.iter_expr, right.iter_expr],
                                                   DictExpr({f'concat({self.iter_expr.key}, {right.iter_expr.key})':
                                                                 f'{self.iter_expr.val} * {right.iter_expr.val}'})))
            self.history_name.append(merged_name)
            self.operations.append(OpExpr('pysdql_merge_by_concatexpr', result))
            new_cols = self.cols + right.cols
            return relation(name=merged_name, cols=new_cols, inherit_from=self).inherit(right)

    def optimized_merge(self, right, left_on, right_on):
        if not type(right) == relation:
            raise TypeError()

        if type(left_on) != type(right_on):
            raise TypeError()

        if type(left_on) == list:
            if type(right_on) == list:
                if len(left_on) != len(right_on):
                    raise ValueError()
            else:
                raise TypeError()

        part_dict = {}

        if type(left_on) == str and type(right_on) == str:
            rec = RecExpr({right_on: f'{right.iter_expr.key}.{right_on}'})
            part_dict[rec] = DictExpr({right.iter_expr.key: right.iter_expr.val})

        for i in self.using_col:
            print(i)

        part_name = f'part_{right.name[0]}'
        part_key = 'p_k'
        part_val = 'p_v'
        part_iter = f'sum(<{part_key}, {part_val}> in {part_name}(<{right_on}={self.iter_expr.key}.{left_on}>))'
        part_var = VarExpr(part_name, IterStmt(right.iter_expr, DictExpr(part_dict)))
        self.history_name.append(part_name)
        self.operations.append(OpExpr('relation_optimized_merge_part_right', part_var))

        new_name = self.gen_merged_name()
        result = VarExpr(new_name, IterStmt([self.iter_expr, part_iter],
                                            DictExpr({f'concat({self.iter_expr.key}, {part_key})':
                                                          f'{self.iter_expr.val} * {part_val}'})))
        self.history_name.append(new_name)
        self.operations.append(OpExpr('relation_optimized_merge_result', result))
        '''
        let part_c = sum(<c_k,c_v> in customer) { c_k.c_custkey -> {c_k->c_v} } in
        sum(<o_k,o_v> in orders) sum(<p_k,p_v> in part_c(o_k.o_custkey)) 
        { concat(o_k,p_k)->o_v*p_v }
        '''
        new_cols = self.cols + right.cols
        return relation(name=new_name,
                        cols=new_cols,
                        inherit_from=self).inherit(right)

    def inherit(self, other):
        """

        pysdql.relation :param other:
        :return:
        """
        if not (type(other) == relation or type(other) == GroupbyExpr):
            raise TypeError('Only inherit from pysdql.Relation or GroupbyExpr')

        # Inherit variable names
        self.history_name += [other.name] + other.history_name

        # Inherit operations
        if not self.operations:
            self.operations = other.operations
        else:
            for i in self.operations:
                if i in other.operations:
                    self.operations.remove(i)
            self.operations = other.operations + self.operations

        return self

    def exists(self, on):
        # count_name = f'count_{self.name}'
        # result = VarExpr(count_name, CompoExpr(self.iter_expr, self.iter_expr.val))
        # self.operations.append(OpExpr('relation_exists', result))
        # return CondUnit(count_name, '>', 0, inherit_from=self)
        # return CondExpr(self.name, '==', '{ }', inherit_from=self)

        # tmp_dict = {}
        # for c in self.groupby_cols:
        #     tmp_dict[c] = f'{self.last_iter.key}.{c}'
        #
        # tmp_key = RecExpr(tmp_dict)
        # tmp_val = DictExpr({self.last_iter.key: self.last_iter.val})
        #
        # nested_dict = VarExpr(name=self.nested_dict_name,
        #                       data=IterStmt(self.last_iter, DictExpr({tmp_key: tmp_val})))
        #
        # self.history_name.append(self.nested_dict_name)
        # self.operations.append(OpExpr('groupby_nested_dict', nested_dict))
        pass

    def not_exists(self, on):
        # count_name = f'count_{self.name}'
        # result = VarExpr(count_name, CompoExpr(self.iter_expr, self.iter_expr.val))
        # self.operations.append(OpExpr('relation_exists', result))
        # return CondUnit(count_name, '==', 0, inherit_from=self)
        # return CondExpr(self.name, '!=', '{ }', inherit_from=self)
        pass

    def case(self, when, then_case, else_case):
        return CaseExpr(self, when, then_case, else_case)

    @property
    def sdql_expr(self):
        expr_str = f'\n'.join([f'{i}' for i in self.operations])
        expr_str += f'\n{self.name}'
        return expr_str

    def count(self):
        next_name = f'count_{self.name}'
        var = VarExpr(next_name, IterStmt(self.iter_expr, self.iter_expr.val))
        self.history_name.append(next_name)
        self.operations.append(OpExpr('relation_count', var))
        return relation(name=next_name,
                        inherit_from=self)

    def head(self, size):
        next_name = self.gen_tmp_name()
        output = VarExpr(next_name, f'ext(`TopN`, {size})')

        self.history_name.append(next_name)
        self.operations.append(OpExpr('relation_head', output))

        return relation(name=next_name,
                        inherit_from=self)

    def sub_query(self, other, super_key: str, sub_key: str):
        if type(other) == relation:
            for o in other.operations:
                print(o.op_str, o.op_obj, type(o.op_obj))

        return self
