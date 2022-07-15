import os
import string

from pysdql.core.dtypes.ArrayExpr import ArrayExpr
from pysdql.core.dtypes.CaseExpr import CaseExpr
from pysdql.core.dtypes.ColumnExpr import ColExpr
from pysdql.core.dtypes.IsinExpr import IsinExpr
from pysdql.core.dtypes.IterationExpr import IterExpr
from pysdql.core.dtypes.ColumnUnit import ColUnit
from pysdql.core.dtypes.ConditionalUnit import CondUnit
from pysdql.core.dtypes.ConditionalExpr import CondExpr
from pysdql.core.dtypes.CompositionExpr import CompoExpr
from pysdql.core.dtypes.DictionaryExpr import DictExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.RecordExpr import RecExpr
from pysdql.core.dtypes.SDict import sdict
from pysdql.core.dtypes.SetExpr import SetExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.GroupbyExpr import GroupbyExpr


class relation:
    def __init__(self, name, data=None, cols=None, inherit_from=None, operations=None):
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

        if inherit_from:
            self.inherit(inherit_from)

        self.iter_expr = IterExpr(self.name)

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
        print(f'let {name} = {self.name} in')
        return relation(name=name)

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
        if item.inherit_from:
            self.inherit(item.inherit_from)
        item = item.new_cond(self.iter_expr.key)
        cond_expr = CondExpr(conditions=item,
                             then_case=DictExpr({self.iter_expr.key: 1}),
                             else_case=DictExpr({}),
                             new_iter=self.iter_expr.key)
        compo_expr = CompoExpr(iter_expr=self.iter_expr,
                               any_expr=cond_expr)

        var_name = self.gen_tmp_name()

        result = relation(name=var_name,
                          # data=self.data,
                          # cols=self.cols,
                          # compo_expr=compo_expr,
                          inherit_from=self)

        self.operations.append(OpExpr('relation_selection', VarExpr(var_name, compo_expr)))
        return result

    def projection(self, cols):
        tmp_d = {}
        for i in cols:
            tmp_d[i] = f'{self.iter_expr.key}.{i}'
        result = relation(name=self.gen_tmp_name(),
                          inherit_from=self
                          )
        print(result)
        return result

    def get_col(self, col_name):
        return ColUnit(self, col_name)

    def select_isin(self, isin_expr: IsinExpr):
        u1 = isin_expr.unit1
        u2 = isin_expr.unit2
        new_name = self.gen_tmp_name()
        print(f'let {new_name} = {self.iter_expr} {u2.relation.iter_expr} '
              f'if ({self.iter_expr.key}.{u1.name} == {u2.relation.iter_expr.key}.{u2.name}) '
              f'then {{ {self.iter_expr.key} }} else {{ }} in')

        return relation(name=new_name, inherit_from=self)

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
        if type(item) == IsinExpr:
            return self.select_isin(item)

    def __setitem__(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        if type(value) == CaseExpr:
            self.name = self.gen_tmp_name()
            value.set(key, self.name, self.iter_expr)
            self.iter_expr = IterExpr(self.name)
            return
        if type(value) == ColUnit or type(value) == ColExpr:
            return self.col_rename(from_col=value, to_col=ColUnit(relation=self, col_name=key))

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
        result = relation(name=new_name,
                          cols=self.cols,
                          compo_expr=CompoExpr(iter_expr=self.iter_expr,
                                               any_expr=SetExpr(RecExpr(kv_pair=rename_dict))
                                               ),
                          inherit_from=self)

        print(result)

        self.update_from_relation(result)
        return result

    @property
    def expr(self) -> str:
        return f'{self.name}'

    def __repr__(self):
        return self.expr

    def show(self):
        if self.data:
            print(f'let {self.name} = {self.data}')
        print(self.name)

    def update_from_relation(self, r):
        """
        Update all properties of 'self' from another relation 'r'
        :param r:
        :return:
        """
        self.name = r.name
        self.cols = r.cols
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

        return relation(name=self.gen_tmp_name(),
                        cols=self.cols,
                        compo_expr=CompoExpr(iter_expr=self.iter_expr,
                                             any_expr=SetExpr(RecExpr(tmp_dict))
                                             ),
                        inherit_from=self
                        )

    def groupby(self, cols: list):
        """
        let tmp = sum (<l_k, l_v> in lineitem) { <l_returnflag=l_k.l_returnflag, l_linestatus=l_k.l_linestatus> -> {l_k -> l_v} }
        in let tmpa = sum(<t_k, t_v> in tmp) { <l_returnflag=t_k.l_returnflag, l_linestatus=t_k.l_linestatus, group=t_v> }
        """
        var_name = self.gen_tmp_name()
        return GroupbyExpr(name=var_name,
                              groupby_from=self,
                              groupby_cols=cols)

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
        """
        This function produce an Array of values.
        :param aggr_func:
        :return:
        """
        new_name = self.gen_tmp_name()

        tmp_dict = {}
        avg_n = 1
        for aggr_key in aggr_func.keys():
            aggr_val = aggr_func[aggr_key]
            aggr_calc = aggr_key
            if type(aggr_calc) == ColUnit or type(aggr_calc) == ColExpr:
                aggr_calc = aggr_calc.new_expr(f'{self.iter_expr.key}')
            if aggr_val == 'sum':
                tmp_dict[aggr_key] = f'{aggr_calc} * {self.iter_expr.val}'
            if aggr_val == 'count':
                tmp_dict[aggr_key] = f'{self.iter_expr.val}'
            if aggr_val == 'avg':
                print(f'let sc_{avg_n} = <s=({aggr_calc} * {self.iter_expr.val}), c={self.iter_expr.val}>')
                tmp_dict[aggr_key] = f'(sc_{avg_n}.s / sc_{avg_n}.c)'
                avg_n += 1

        aggr_array = ', '.join(tmp_dict.values())

        print(f'let {new_name} = {self.iter_expr} [| {aggr_array} |]')

        return ArrayExpr(new_name, list(tmp_dict.values()))

    def aggr_kwargs_parse(self, aggr_dict: dict):
        result_dict = {}

        aggr_record = self.gen_tmp_name()

        tmp_name = self.gen_tmp_name()
        tmp_iter_expr = IterExpr(tmp_name)

        tmp_dict = {}
        for aggr_key in aggr_dict.keys():
            aggr_val = aggr_dict[aggr_key]
            if type(aggr_val) == ColUnit:
                result_dict[aggr_key] = f'{aggr_record}.{aggr_dict[aggr_key].name}'
            if type(aggr_val) == tuple:
                if type(aggr_dict[aggr_key][0]) == ColUnit or type(aggr_dict[aggr_key][0]) == ColExpr:
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

        aggr_record = VarExpr(tmp_name, CompoExpr(self.iter_expr, RecExpr(tmp_dict)))
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
        return new_r.aggr(aggr_func, *args, **kwargs)

    def access(self):
        return self

    def merge(self, on: CondUnit, *args):
        pass

    def optimized_merge(self, other, on):
        if not type(other) == relation:
            raise TypeError()
        tmp_d = DictExpr({on.get_1st(): DictExpr({self.iter_expr.key: self.iter_expr.val})})
        tmp_r = relation(name=f'part_{self.name}',
                         inherit_from=self
                         )
        print(tmp_r)

        result_d = DictExpr(
            {f'concat({other.iter_expr.key}, {tmp_r.iter_expr.key})': f'{other.iter_expr.val} * {tmp_r.iter_expr.val}'})
        result_r = relation(name=self.gen_tmp_name(),
                            inherit_from=tmp_r)
        print(result_r)

        '''
        let part_c = sum(<c_k,c_v> in customer) { c_k.c_custkey -> {c_k->c_v} } in
        sum(<o_k,o_v> in orders) sum(<p_k,p_v> in part_c(o_k.o_custkey)) 
        { concat(o_k,p_k)->o_v*p_v }
        '''

        return result_r

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

    def exists(self):
        count_name = f'count_{self.name}'
        result = VarExpr(count_name, CompoExpr(self.iter_expr, self.iter_expr.val))
        self.operations.append(OpExpr('relation_exists', result))
        return CondUnit(count_name, '>', 0, inherit_from=self)

    def not_exists(self):
        count_name = f'count_{self.name}'
        result = VarExpr(count_name, CompoExpr(self.iter_expr, self.iter_expr.val))
        self.operations.append(OpExpr('relation_exists', result))
        return CondUnit(count_name, '==', 0, inherit_from=self)

    @staticmethod
    def case(when, then_case, else_case):
        return CaseExpr(when, then_case, else_case)

    @property
    def sdql_expr(self):
        expr_str = f'\n'.join([f'{i}' for i in self.operations])
        expr_str += f'\n{self.name}'
        return expr_str
