import string

from pysdql.core.dtypes.IterationExpr import IterExpr
from pysdql.core.dtypes.ColumnUnit import ColUnit
from pysdql.core.dtypes.ConditionalUnit import CondUnit
from pysdql.core.dtypes.ConditionalExpr import CondExpr
from pysdql.core.dtypes.ConstructionExpr import ConstrExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.SetExpr import SetExpr


class Relation:
    def __init__(self, name, cols=None, constr_expr=None, inherit_from=None):
        self.name = name
        self.cols = cols
        self.constr_expr = constr_expr

        self.history_name = []

        if inherit_from:
            self.inherit(inherit_from)

        self.iter_expr = IterExpr(self.name)

    def rename(self, name):
        self.name = name

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

    def selection(self, item: CondUnit):
        cond_expr = CondExpr(conditions=item, then_case=SetExpr(self.iter_expr.key), new_iter=self.iter_expr.key)
        result = Relation(name=self.gen_tmp_name(),
                          cols=self.cols,
                          constr_expr=ConstrExpr(iter_expr=self.iter_expr, any_expr=cond_expr),
                          inherit_from=self)

        print(result.expr)
        return result

    def projection(self, cols):
        tmp_d = {}
        for i in cols:
            tmp_d[i] = f'{self.iter_expr.key}.{i}'
        result = Relation(name=self.gen_tmp_name(),
                          cols=cols,
                          constr_expr=ConstrExpr(iter_expr=self.iter_expr, any_expr=SetExpr(RecExpr(tmp_d))),
                          inherit_from=self
                          )
        print(result.constr_expr)
        return result

    def get_col(self, col_name):
        return ColUnit(self, col_name)

    def __getattr__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

    def __getitem__(self, item):
        if type(item) == CondUnit:
            return self.selection(item)
        if type(item) == str:
            return self.get_col(col_name=item)
        if type(item) == list:
            return self.projection(item)

    def __setitem__(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        self.col_rename(from_col=value, to_col=ColUnit(relation=self, col_name=key))

    def keep_cols(self):
        tmp_dict = {}
        for i in self.cols:
            tmp_dict[i] = ColUnit(relation=self, col_name=i)
        return tmp_dict

    def col_rename(self, from_col, to_col):
        """

        :param from_col: ColExpr
        :param to_col: ColUnit
        :return:
        """

        rename_dict = {to_col.name: from_col}
        rename_dict.update(self.keep_cols())
        new_name = self.gen_tmp_name()
        # new_history_name = [new_name]
        # new_history_name = new_history_name + self.history_name
        result = Relation(name=new_name,
                          cols=self.cols,
                          constr_expr=ConstrExpr(iter_expr=self.iter_expr,
                                                 any_expr=SetExpr(RecExpr(kv_pair=rename_dict))
                                                 ),
                          inherit_from=self)

        result.show()

        self.update_from_relation(result)
        return result

    @property
    def expr(self) -> str:
        if self.constr_expr:
            return f'let {self.name} = {self.constr_expr} in'
        else:
            return f'{self.name}'

    def __repr__(self):
        return self.expr

    def show(self):
        print(self.constr_expr)

    def update_from_relation(self, r):
        """
        Update all properties of 'self' from another relation 'r'
        :param r:
        :return:
        """
        self.name = r.name
        self.cols = r.cols
        self.constr_expr = r.constr_expr
        self.history_name = r.history_name

    def from_cols(self, col_list: list):
        """
        Create a new relation with column list ['col1', 'col2', ..., 'coln']
        :param col_list:
        :return:
        """
        tmp_dict = {}
        for col in col_list:
            tmp_dict[col] = f'{self.iter_expr.key}.{col}'

        return Relation(name=self.gen_tmp_name(),
                        cols=self.cols,
                        constr_expr=ConstrExpr(iter_expr=self.iter_expr,
                                               any_expr=SetExpr(RecExpr(tmp_dict))
                                               ),
                        inherit_from=self
                        )

    def groupby(self, cols: list):
        """
        let tmp = sum (<l_k, l_v> in lineitem) { <l_returnflag=l_k.l_returnflag, l_linestatus=l_k.l_linestatus> -> {l_k -> l_v} }
        in let tmpa = sum(<t_k, t_v> in tmp) { <l_returnflag=t_k.l_returnflag, l_linestatus=t_k.l_linestatus, group=t_v> }
        """
        from pysdql.core.dtypes.GroupbyExpr import GroupbyExpr
        return GroupbyExpr(name=self.gen_tmp_name(),
                           groupby_from=self,
                           groupby_cols=cols,
                           )

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
            print('Aggregation Count')

    def aggr_dict_parse(self, aggr_func: dict):
        tmp_dict = {}
        for aggr_key in aggr_func.keys():
            dict_val = aggr_func[aggr_key]
            if dict_val == 'sum':
                tmp_dict[aggr_key] = f'{self.iter_expr.key}.{aggr_key} * {self.iter_expr.val}'
            if dict_val == 'count':
                pass
            if dict_val == 'avg':
                pass

        tmp_r = Relation(name=self.gen_tmp_name(),
                         cols=self.cols,
                         constr_expr=ConstrExpr(iter_expr=self.iter_expr,
                                                any_expr=SetExpr(RecExpr(kv_pair=tmp_dict))),
                         inherit_from=self)
        tmp_r.show()
        return tmp_r

    def aggr_kwargs_parse(self, aggr_dict: dict):

        tmp_dict = {}
        for aggr_key in aggr_dict.keys():
            aggr_val = aggr_dict[aggr_key]
            if type(aggr_val) == ColUnit:
                tmp_dict[aggr_key] = f'{self.iter_expr.key}.{aggr_val.name}'
            if type(aggr_val) == tuple:
                aggr_calc = aggr_dict[aggr_key][0]
                aggr_flag = aggr_dict[aggr_key][1]
                if aggr_flag == 'sum':
                    tmp_dict[aggr_key] = f'{aggr_calc} * {self.iter_expr.val}'
                if aggr_flag == 'count':
                    tmp_dict[aggr_key] = f'{self.iter_expr.val}'
                if aggr_flag == 'avg':
                    pass

        tmp_r = Relation(name=self.gen_tmp_name(),
                         cols=self.cols,
                         constr_expr=ConstrExpr(iter_expr=self.iter_expr,
                                                any_expr=RecExpr(kv_pair=tmp_dict)),
                         inherit_from=self)
        tmp_r.show()
        return tmp_r

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
        new_r.show()
        return new_r.aggr(aggr_func, *args, **kwargs)

    def access(self):
        return self

    def merge(self, on: CondUnit, *args):
        pass

    def optimized_merge(self, other, on):
        if not type(other) == Relation:
            raise TypeError()
        tmp_d = DictExpr({on.get_1st(): DictExpr({self.iter_expr.key: self.iter_expr.val})})
        tmp_r = Relation(name=f'part_{self.name}',
                         constr_expr=ConstrExpr(iter_expr=self.iter_expr,
                                                any_expr=tmp_d),
                         inherit_from=self
                         )
        print(tmp_r)

        result_d = DictExpr(
            {f'concat({other.iter_expr.key}, {tmp_r.iter_expr.key})': f'{other.iter_expr.val} * {tmp_r.iter_expr.val}'})
        result_r = Relation(name=self.gen_tmp_name(),
                            constr_expr=ConstrExpr(
                                iter_expr=f'{other.iter_expr} sum( {tmp_r.iter_expr.kv_pair} in {tmp_r.name}({on.get_2nd()}) )',
                                any_expr=result_d),
                            inherit_from=tmp_r)
        print(result_r)

        '''
        let part_c = sum(<c_k,c_v> in customer) { c_k.c_custkey -> {c_k->c_v} } in
        sum(<o_k,o_v> in orders) sum(<p_k,p_v> in part_c(o_k.o_custkey)) 
        { concat(o_k,p_k)->o_v*p_v }
        '''

        return result_r

    def inherit(self, other):
        if not type(other) == Relation:
            raise TypeError('Only inherit from pysdql.Relation')

        if other.history_name:
            self.history_name += [other.name] + other.history_name
        else:
            self.history_name += [other.name]

    def exists(self):
        return f'({self.iter_expr} {self.iter_expr.val}) > 0'
