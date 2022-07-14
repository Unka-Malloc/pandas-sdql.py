import string

from pysdql.core.dtypes.ColumnExpr import ColExpr
from pysdql.core.dtypes.ColumnUnit import ColUnit
from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.HavUnit import HavUnit
from pysdql.core.dtypes.IterationExpr import IterExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.SetExpr import SetExpr
from pysdql.core.dtypes.VarExpr import VarExpr


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
                              data=CompoExpr(self.last_iter, DictExpr({tmp_key: tmp_val})))

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
                         data=CompoExpr(self.nested_dict_iter_expr, DictExpr({RecExpr(tmp_dict): 1})))

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
                return self.aggr_str_parse(aggr_func)
            if type(aggr_func) == dict:
                return self.aggr_dict_parse(aggr_func)
        if aggr_args:
            pass
        if aggr_kwargs:
            return self.aggr_kwargs_parse(aggr_kwargs)

    def __getitem__(self, item):
        return self.groupby_from[item]

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
                return self.aggr_str_parse(aggr_func)
            if type(aggr_func) == dict:
                return self.aggr_dict_parse(aggr_func)
        if aggr_args:
            pass
        if aggr_kwargs:
            return self.aggr_kwargs_parse(aggr_kwargs)

    def aggr_str_parse(self, aggr_str):
        if aggr_str == 'count':
            print(f'Aggregation Count on {self.name}')

    def aggr_dict_parse(self, aggr_func: dict):
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
            if type(aggr_key) == ColUnit or type(aggr_key) == ColExpr:
                aggr_calc = f'{aggr_key.name}'
            if dict_val == 'sum':
                tmp_dict[aggr_calc] = f'{aggr_tmp_iter_key}.{aggr_calc} * {self.iter_expr.val}'
                result_dict[aggr_calc] = f'{aggr_tuple_iter_expr.val}.{aggr_calc}'
            if dict_val == 'count':
                pass
            if dict_val == 'avg':
                pass

        print(
            f'let {aggr_tuple_name} = {self.iter_expr} {aggr_tmp_iter_expr} '
            f'{{ {RecExpr(self.cols_in_rec())} -> {RecExpr(tmp_dict)} }} in')

        self.history_name += [aggr_tuple_name]

        print(f'let agg_r = {aggr_tuple_iter_expr} {{ {RecExpr(result_dict)} }} in')

        from pysdql import relation
        return relation('agg_r')

    def aggr_kwargs_parse(self, aggr_dict: dict):
        result_dict = {}

        aggr_tuple_name = self.gen_tmp_name()
        aggr_tuple_iter_expr = IterExpr(aggr_tuple_name)

        tmp_cols = []
        tmp_cols += self.groupby_cols

        for k in aggr_dict.keys():
            v = aggr_dict[k]
            if type(v) == ColUnit:
                tmp_cols.remove(v.name)

        for c in tmp_cols:
            result_dict[c] = f'{aggr_tuple_iter_expr.key}.{c}'

        aggr_tmp_iter_key = 'g_k'
        aggr_tmp_iter_val = 'g_v'
        aggr_tmp_iter_expr = f'sum (<{aggr_tmp_iter_key}, {aggr_tmp_iter_val}> in {self.iter_expr.key}.group)'

        aggr_tuple_dict = {}
        for aggr_key in aggr_dict.keys():
            aggr_val = aggr_dict[aggr_key]
            if type(aggr_val) == ColUnit:
                result_dict[aggr_key] = f'{aggr_tuple_iter_expr.key}.{aggr_dict[aggr_key].name}'
            if type(aggr_val) == tuple:
                if type(aggr_dict[aggr_key][0]) == ColUnit or type(aggr_dict[aggr_key][0]) == ColExpr:
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
                                data=CompoExpr([self.iter_expr, aggr_tmp_iter_expr],
                                               DictExpr(
                                                   {RecExpr(self.cols_in_rec()):
                                                        RecExpr(aggr_tuple_dict)})
                                               )
                                )

        self.groupby_aggr_parse_nested_dict = parse_nested_dict
        self.history_name.append(aggr_tuple_name)
        self.operations.append(OpExpr('groupby_aggr_parse_nested_dict', parse_nested_dict))

        next_name = f'aggr_{self.groupby_from.name}'

        result = VarExpr(name=next_name,
                         data=CompoExpr(aggr_tuple_iter_expr, DictExpr({RecExpr(result_dict): 1})))
        self.groupby_aggr_result = result
        self.history_name.append(next_name)
        self.operations.append(OpExpr('groupby_aggr_result', result))

        from pysdql import relation
        return relation(name=next_name,
                        inherit_from=self)

    def filter(self, func):
        func(HavUnit(iter_expr=self.iter_expr, groupby_cols=self.groupby_cols))

        from pysdql import relation
        return relation(name='hvR')

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
