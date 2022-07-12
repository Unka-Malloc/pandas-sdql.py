import string

from pysdql.core.dtypes.ColumnExpr import ColExpr
from pysdql.core.dtypes.ColumnUnit import ColUnit
from pysdql.core.dtypes.ConstructionExpr import ConstrExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.HavUnit import HavUnit
from pysdql.core.dtypes.IterationExpr import IterExpr
from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.SetExpr import SetExpr


class GroupbyExpr:
    def __init__(self, name, groupby_from, groupby_cols):
        self.name = name
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols

        self.history_name = []

        self.iter_expr = IterExpr(self.name)
        self.last_iter = self.groupby_from.iter_expr
        self.tmp_name = self.gen_tmp_name()
        self.tmp_iter_expr = IterExpr(self.tmp_name)

        self.show_tmp()
        self.show()

    def cols_in_rec(self) -> dict:
        tmp_d = {}
        for c in self.groupby_cols:
            tmp_d[c] = f'{self.iter_expr.key}.{c}'
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

    def show_tmp(self):
        # let tmp = sum (<l_k, l_v> in lineitem) { <l_returnflag=l_k.l_returnflag, l_linestatus=l_k.l_linestatus> -> {l_k -> l_v} } in
        tmp_dict = {}
        for c in self.groupby_cols:
            tmp_dict[c] = f'{self.last_iter.key}.{c}'
        tmp_key = RecExpr(tmp_dict)
        tmp_val = DictExpr({self.last_iter.key: self.last_iter.val})
        print(f'let {self.tmp_name} = {self.last_iter} {DictExpr({tmp_key: tmp_val})} in')

        self.history_name += [self.name]

    def show(self):
        #  sum(<t_k, t_v> in tmp) { <l_returnflag=t_k.l_returnflag, l_linestatus=t_k.l_linestatus, group=t_v> }
        new_d = {}
        for c in self.groupby_cols:
            new_d[c] = f'{self.tmp_iter_expr.key}.{c}'
        new_d['group'] = f'{self.tmp_iter_expr.val}'

        self.history_name += [self.tmp_name]

        print(f'let {self.name} = {self.tmp_iter_expr} {SetExpr(RecExpr(new_d))} in')

        self.iter_expr = IterExpr(self.name)

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

        # from pysdql import Relation
        # tmp_r = Relation(name=self.gen_tmp_name(),
        #                  cols=self.groupby_from.cols,
        #                  constr_expr=ConstrExpr(iter_expr=self.iter_expr,
        #                                         any_expr=SetExpr(RecExpr(kv_pair=tmp_dict))),
        #                  inherit_from=self.groupby_from)
        # print(tmp_r)
        from pysdql import Relation
        return Relation('agg_r')

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
                    result_dict[aggr_key] = f'({aggr_tuple_iter_expr.val}.{aggr_key}_sum / {aggr_tuple_iter_expr.val}.{aggr_key}_count)'

        print(
            f'let {aggr_tuple_name} = {self.iter_expr} {aggr_tmp_iter_expr} {{ {RecExpr(self.cols_in_rec())} -> {RecExpr(aggr_tuple_dict)} }}')

        self.history_name += [aggr_tuple_name]

        print(f'let agg_r = {aggr_tuple_iter_expr} {{ {RecExpr(result_dict)} }} in')

        from pysdql import Relation
        return Relation('agg_r')

    def filter(self, func):
        func(HavUnit(iter_expr=self.iter_expr, groupby_cols=self.groupby_cols))

        from pysdql.core.dtypes.structure.Relation import Relation
        return Relation(name='hvR')
