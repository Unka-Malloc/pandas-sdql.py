import string

from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.DictExpr import DictExpr
from pysdql.core.dtypes.HavUnit import HavUnit
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.SetExpr import SetExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.CondStmt import CondStmt


class GroupbyExpr:
    def __init__(self, name, groupby_from, groupby_cols):
        """

        :param name: str
        :param groupby_from: pysdql.relation
        :param groupby_cols: list
        """
        self.name = name
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols

        self.history_name = []
        self.operations = []
        self.operations += groupby_from.operations

        self.iter_for_agg = self.groupby_from.iter_expr

        self.iter_expr = IterExpr(self.name)
        self.last_iter = self.groupby_from.iter_expr
        self.nested_dict_name = self.gen_tmp_name()
        self.nested_dict_iter_expr = IterExpr(self.nested_dict_name)

        # let tmpa = sum (<l_k, l_v> in lmp) { < l_returnflag=l_k.l_returnflag, l_linestatus=l_k.l_linestatus > -> { l_k -> l_v } }
        self.groupby_nested_dict = self.get_nested_dict()
        # let tmp = sum (<ta_k, ta_v> in tmpa) { < l_returnflag=ta_k.l_returnflag, l_linestatus=ta_k.l_linestatus, group=ta_v > }
        self.groupby_result = self.get_result()

        # let tmpb = sum (<t_k, t_v> in tmp) sum (<g_k, g_v> in t_k.group) { < l_returnflag=t_k.l_returnflag, l_linestatus=t_k.l_linestatus > -> < sum_qty=g_k.l_quantity * g_v, sum_base_price=g_k.l_extendedprice * g_v, sum_disc_price=(g_k.l_extendedprice * (1 - g_k.l_discount)) * g_v, sum_charge=((g_k.l_extendedprice * (1 - g_k.l_discount)) * (1 + g_k.l_tax)) * g_v, avg_qty_sum=g_k.l_quantity * g_v, avg_qty_count=g_v, avg_price_sum=g_k.l_extendedprice * g_v, avg_price_count=g_v, avg_disc_sum=g_k.l_discount * g_v, avg_disc_count=g_v, count_order=g_v > }
        self.groupby_aggr_parse_nested_dict = None
        # let agg_r = sum (<tb_k, tb_v> in tmpb) { < l_returnflag=tb_k.l_returnflag, l_linestatus=tb_k.l_linestatus, sum_qty=tb_v.sum_qty, sum_base_price=tb_v.sum_base_price, sum_disc_price=tb_v.sum_disc_price, sum_charge=tb_v.sum_charge, avg_qty=(tb_v.avg_qty_sum / tb_v.avg_qty_count), avg_price=(tb_v.avg_price_sum / tb_v.avg_price_count), avg_disc=(tb_v.avg_disc_sum / tb_v.avg_disc_count), count_order=tb_v.count_order > } in
        self.groupby_aggr_result = None

    def cols_in_rec(self, key=None) -> dict:
        if key is None:
            key = self.iter_expr.key
        tmp_d = {}
        for c in self.groupby_cols:
            tmp_d[c] = f'{key}.{c}'
        return tmp_d

    def gen_tmp_name(self, noname=None):
        if noname is None:
            noname = []
        name_list = ['tmp']
        for i in list(string.ascii_lowercase):
            name_list.append(f'tmp{i}')

        dup_list = [self.name, self.groupby_from.name] + self.history_name + self.groupby_from.history_name + noname

        for tmp_name in name_list:
            if tmp_name not in dup_list:
                return tmp_name

    def get_nested_dict(self):
        # let tmp = sum (<l_k, l_v> in lineitem) { <l_returnflag=l_k.l_returnflag, l_linestatus=l_k.l_linestatus> -> {l_k -> l_v} } in
        tmp_dict = {}
        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.last_iter.key}.{c}'

        tmp_key = RecExpr(tmp_dict)
        tmp_val = DictExpr({self.last_iter.key: self.last_iter.val})

        nested_dict = VarExpr(name=self.nested_dict_name,
                              data=IterStmt(self.last_iter, DictExpr({tmp_key: tmp_val})))

        self.history_name.append(self.nested_dict_name)
        self.operations.append(OpExpr('groupby_nested_dict', nested_dict))

        return nested_dict

    def get_result(self):
        #  sum(<t_k, t_v> in tmp) { <l_returnflag=t_k.l_returnflag, l_linestatus=t_k.l_linestatus, group=t_v> }
        tmp_dict = {}
        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.nested_dict_iter_expr.key}.{c}'
        tmp_dict['group'] = f'{self.nested_dict_iter_expr.val}'

        result = VarExpr(name=self.name,
                         data=IterStmt(self.nested_dict_iter_expr, DictExpr({RecExpr(tmp_dict): 1})))

        self.history_name.append(self.name)
        self.operations.append(OpExpr('groupby_result', result))

        return result

    def aggr(self, aggr_func=None, *aggr_args, **aggr_kwargs):
        """
        Aggregation
        :param aggr_func:
        :param args:
        :param kwargs:
        :return:
        """
        if aggr_func:
            if type(aggr_func) == str:
                return self.aggregate_str_parse(aggr_func)
            if type(aggr_func) == dict:
                return self.aggregate_dict_parse(aggr_func)
        if aggr_args:
            pass
        if aggr_kwargs:
            return self.aggregate_kwargs_parse(aggr_kwargs)

    def agg(self, agg_func=None, *agg_args, **agg_kwargs):
        if agg_func:
            if type(agg_func) == str:
                return self.optimized_agg_str_parse(agg_func)
            if type(agg_func) == dict:
                return self.optimized_agg_dict_parse(agg_func)
        if agg_args:
            pass
        if agg_kwargs:
            return self.optimized_agg_kwargs_parse(agg_kwargs)

    def optimized_agg_str_parse(self, agg_str):
        pass

    def optimized_agg_dict_parse(self, agg_func):
        pass

    def optimized_agg_kwargs_parse(self, agg_dict):
        if self.groupby_result:
            self.operations.pop()
            self.history_name.pop()
        if self.groupby_nested_dict:
            self.operations.pop()
            self.history_name.pop()

        result_dict = {}

        aggr_tuple_name = self.gen_tmp_name()
        aggr_tuple_iter_expr = IterExpr(aggr_tuple_name)

        tmp_cols = []
        tmp_cols += self.groupby_cols

        for k in agg_dict.keys():
            v = agg_dict[k]
            if type(v) == ColEl:
                if v.name in tmp_cols:
                    tmp_cols.remove(v.name)

        for c in tmp_cols:
            result_dict[c] = f'{aggr_tuple_iter_expr.key}.{c}'

        promoted_cols = {}

        aggr_tuple_dict = {}
        for aggr_key in agg_dict.keys():
            aggr_val = agg_dict[aggr_key]
            if type(aggr_val) == ColEl:
                result_dict[aggr_key] = f'{aggr_tuple_iter_expr.key}.{agg_dict[aggr_key].name}'
            if type(aggr_val) == tuple:
                if type(agg_dict[aggr_key][0]) == ColEl or type(agg_dict[aggr_key][0]) == ColExpr:
                    aggr_calc = agg_dict[aggr_key][0].new_expr(f'{self.iter_for_agg.key}')
                else:
                    aggr_calc = agg_dict[aggr_key][0]
                aggr_flag = agg_dict[aggr_key][1]
                if aggr_flag == 'sum':
                    aggr_tuple_dict[aggr_key] = f'{aggr_calc} * {self.iter_for_agg.val}'
                    result_dict[aggr_key] = f'{aggr_tuple_iter_expr.val}.{aggr_key}'
                if aggr_flag == 'count':
                    aggr_tuple_dict[aggr_key] = f'{self.iter_for_agg.val}'
                    result_dict[aggr_key] = f'{aggr_tuple_iter_expr.val}.{aggr_key}'
                if aggr_flag == 'avg':
                    aggr_tuple_dict[f'{aggr_key}_sum'] = f'{aggr_calc} * {self.iter_for_agg.val}'
                    aggr_tuple_dict[f'{aggr_key}_count'] = f'{self.iter_for_agg.val}'
                    result_dict[aggr_key] = f'({aggr_tuple_iter_expr.val}.{aggr_key}_sum ' \
                                            f'/ {aggr_tuple_iter_expr.val}.{aggr_key}_count)'
                if aggr_flag == 'min':
                    promoted_cols[aggr_key] = 'promote[mnpr]'
                    aggr_tuple_dict[aggr_key] = f'promote[mnpr]({aggr_calc}) * promote[mnpr]({self.iter_for_agg.val})'
                    result_dict[aggr_key] = f'{aggr_tuple_iter_expr.val}.{aggr_key}'
                if aggr_flag == 'max':
                    pass
        parse_nested_dict = VarExpr(name=aggr_tuple_name,
                                    data=IterStmt(self.iter_for_agg,
                                                  DictExpr(
                                                       {RecExpr(self.cols_in_rec(self.iter_for_agg.key)):
                                                            RecExpr(aggr_tuple_dict)})
                                                  )
                                    )

        self.groupby_aggr_parse_nested_dict = parse_nested_dict
        self.history_name.append(aggr_tuple_name)
        self.operations.append(OpExpr('groupby_optimized_agg_parse_nested_dict', parse_nested_dict))

        next_name = f'agg_{self.groupby_from.name}'

        result = VarExpr(name=next_name,
                         data=IterStmt(aggr_tuple_iter_expr, DictExpr({RecExpr(result_dict): 1})))
        self.groupby_aggr_result = result
        self.history_name.append(next_name)
        self.operations.append(OpExpr('groupby_optimized_agg_result', result))

        from pysdql import relation
        output_cols = list(agg_dict.keys())
        return relation(name=next_name,
                        cols=output_cols,
                        inherit_from=self,
                        promoted_cols=promoted_cols)

    def __getitem__(self, item):
        return self.groupby_from[item]

    def aggregate(self, aggr_func=None, *aggr_args, **aggr_kwargs):
        """
        Aggregation
        :param aggr_func:
        :param args:
        :param kwargs:
        :return:
        """
        if aggr_func:
            if type(aggr_func) == str:
                return self.aggregate_str_parse(aggr_func)
            if type(aggr_func) == dict:
                return self.aggregate_dict_parse(aggr_func)
        if aggr_args:
            pass
        if aggr_kwargs:
            return self.aggregate_kwargs_parse(aggr_kwargs)

    def aggregate_str_parse(self, aggr_str):
        if aggr_str == 'count':
            print(f'Aggregation Count on {self.name}')

    def aggregate_dict_parse(self, aggr_func: dict):
        tmp_dict = {}
        result_dict = {}

        aggr_tuple_name = self.gen_tmp_name()
        aggr_tuple_iter_expr = IterExpr(aggr_tuple_name)

        for c in self.groupby_cols:
            result_dict[c] = f'{aggr_tuple_iter_expr.key}.{c}'

        aggr_tmp_iter_key = 'g_k'
        aggr_tmp_iter_val = 'g_v'
        aggr_tmp_iter_expr = f'sum (<{aggr_tmp_iter_key}, {aggr_tmp_iter_val}> in {self.iter_expr.key}.group)'

        for aggr_key in aggr_func.keys():
            dict_val = aggr_func[aggr_key]
            aggr_calc = aggr_key
            if type(aggr_key) == ColEl or type(aggr_key) == ColExpr:
                aggr_calc = f'{aggr_key.name}'
            if dict_val == 'sum':
                tmp_dict[f'{aggr_calc}_sum'] = f'{aggr_tmp_iter_key}.{aggr_calc} * {self.iter_expr.val}'
                result_dict[f'{aggr_calc}_sum'] = f'{aggr_tuple_iter_expr.val}.{aggr_calc}_sum'
            if dict_val == 'count':
                tmp_dict[f'{aggr_calc}_count'] = f'{self.iter_expr.val}'
                result_dict[f'{aggr_calc}_count'] = f'{aggr_tuple_iter_expr.val}.{aggr_calc}_count'
            if dict_val == 'avg':
                tmp_dict[f'{aggr_calc}_sum'] = f'{aggr_tmp_iter_key}.{aggr_calc} * {self.iter_expr.val}'
                tmp_dict[f'{aggr_calc}_count'] = f'{self.iter_expr.val}'
                result_dict[f'{aggr_calc}_avg'] = f'{aggr_tuple_iter_expr.val}.{aggr_calc}_sum' \
                                                  f' / ' \
                                                  f'{aggr_tuple_iter_expr.val}.{aggr_calc}_count'

        parse_nested_dict = VarExpr(name=aggr_tuple_name,
                                    data=IterStmt([self.iter_expr, aggr_tmp_iter_expr],
                                                  DictExpr(
                                                       {RecExpr(self.cols_in_rec()):
                                                            RecExpr(tmp_dict)})
                                                  )
                                    )

        self.groupby_aggr_parse_nested_dict = parse_nested_dict
        self.history_name.append(aggr_tuple_name)
        self.operations.append(OpExpr('groupby_aggregate_parse_nested_dict', parse_nested_dict))

        next_name = f'aggr_{self.groupby_from.name}'

        result = VarExpr(name=next_name,
                         data=IterStmt(aggr_tuple_iter_expr, DictExpr({RecExpr(result_dict): 1})))
        self.groupby_aggr_result = result
        self.history_name.append(next_name)
        self.operations.append(OpExpr('groupby_aggregate_result', result))

        from pysdql import relation
        output_cols = list(aggr_func.keys())
        return relation(name=next_name,
                        cols=output_cols,
                        inherit_from=self)

    def aggregate_kwargs_parse(self, aggr_dict: dict):
        result_dict = {}

        aggr_tuple_name = self.gen_tmp_name()
        aggr_tuple_iter_expr = IterExpr(aggr_tuple_name)

        tmp_cols = []
        tmp_cols += self.groupby_cols

        for k in aggr_dict.keys():
            v = aggr_dict[k]
            if type(v) == ColEl:
                if v.name in tmp_cols:
                    tmp_cols.remove(v.name)

        for c in tmp_cols:
            result_dict[c] = f'{aggr_tuple_iter_expr.key}.{c}'

        aggr_tmp_iter_key = 'g_k'
        aggr_tmp_iter_val = 'g_v'
        aggr_tmp_iter_expr = f'sum (<{aggr_tmp_iter_key}, {aggr_tmp_iter_val}> in {self.iter_expr.key}.group)'

        aggr_tuple_dict = {}
        for aggr_key in aggr_dict.keys():
            aggr_val = aggr_dict[aggr_key]
            if type(aggr_val) == ColEl:
                result_dict[aggr_key] = f'{aggr_tuple_iter_expr.key}.{aggr_dict[aggr_key].name}'
            if type(aggr_val) == tuple:
                if type(aggr_dict[aggr_key][0]) == ColEl or type(aggr_dict[aggr_key][0]) == ColExpr:
                    aggr_calc = aggr_dict[aggr_key][0].new_expr(f'{aggr_tmp_iter_key}')
                else:
                    aggr_calc = aggr_dict[aggr_key][0]
                aggr_flag = aggr_dict[aggr_key][1]
                if aggr_flag == 'sum':
                    aggr_tuple_dict[aggr_key] = f'{aggr_calc} * {aggr_tmp_iter_val}'
                    result_dict[aggr_key] = f'{aggr_tuple_iter_expr.val}.{aggr_key}'
                if aggr_flag == 'count':
                    aggr_tuple_dict[aggr_key] = f'{aggr_tmp_iter_val}'
                    result_dict[aggr_key] = f'{aggr_tuple_iter_expr.val}.{aggr_key}'
                if aggr_flag == 'avg':
                    aggr_tuple_dict[f'{aggr_key}_sum'] = f'{aggr_calc} * {aggr_tmp_iter_val}'
                    aggr_tuple_dict[f'{aggr_key}_count'] = f'{aggr_tmp_iter_val}'
                    result_dict[
                        aggr_key] = f'({aggr_tuple_iter_expr.val}.{aggr_key}_sum / {aggr_tuple_iter_expr.val}.{aggr_key}_count)'

        parse_nested_dict = VarExpr(name=aggr_tuple_name,
                                    data=IterStmt([self.iter_expr, aggr_tmp_iter_expr],
                                                  DictExpr(
                                                       {RecExpr(self.cols_in_rec()):
                                                            RecExpr(aggr_tuple_dict)})
                                                  )
                                    )

        self.groupby_aggr_parse_nested_dict = parse_nested_dict
        self.history_name.append(aggr_tuple_name)
        self.operations.append(OpExpr('groupby_aggregate_parse_nested_dict', parse_nested_dict))

        next_name = f'aggr_{self.groupby_from.name}'

        result = VarExpr(name=next_name,
                         data=IterStmt(aggr_tuple_iter_expr, DictExpr({RecExpr(result_dict): 1})))
        self.groupby_aggr_result = result
        self.history_name.append(next_name)
        self.operations.append(OpExpr('groupby_aggregate_result', result))

        from pysdql import relation
        output_cols = list(aggr_dict.keys())
        return relation(name=next_name,
                        cols=output_cols,
                        inherit_from=self)

    def filter(self, func):
        output = func(HavUnit(groupby_expr=self))

        from pysdql import relation
        return relation(name=output.name,
                        inherit_from=self)

    @property
    def sdql_expr(self):
        expr_str = f'\n'.join([f'{i}' for i in self.operations])
        expr_str += f'\n{self.name}'
        return expr_str

    @property
    def expr(self):
        return self.name

    def __repr__(self):
        return self.expr

    def inherit(self, other):
        """

        pysdql.relation :param other:
        :return:
        """
        from pysdql import relation
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
