from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.sdql_ir import ConstantExpr


class DataFrameGroupBy:
    def __init__(self, groupby_from, groupby_cols):
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols

    def agg(self, agg_func=None, *agg_args, **agg_kwargs):
        if agg_func:
            if type(agg_func) == str:
                return self.agg_str_parse(agg_func)
            if type(agg_func) == dict:
                return self.agg_dict_parse(agg_func)
        if agg_args:
            pass
        if agg_kwargs:
            return self.agg_kwargs_parse(agg_kwargs)

    def agg_str_parse(self, agg_str):
        pass

    def agg_dict_parse(self, agg_dict):
        pass

    def agg_kwargs_parse(self, agg_tuple_dict):
        agg_dict = {}

        for agg_key in agg_tuple_dict.keys():
            agg_val = agg_tuple_dict[agg_key]
            if type(agg_val) == tuple:
                agg_calc = self.groupby_from.key_access(agg_tuple_dict[agg_key][0])
            else:
                raise ValueError()
            agg_flag = agg_tuple_dict[agg_key][1]

            if agg_flag == 'sum':
                agg_dict[agg_key] = agg_calc
            if agg_flag == 'count':
                agg_dict[agg_key] = ConstantExpr(1)
            if callable(agg_flag):
                agg_dict[agg_key] = ConstantExpr(1)

        groupby_agg = GroupByAgg(groupby_from=self.groupby_from,
                                 groupby_cols=self.groupby_cols,
                                 agg_dict=agg_dict,
                                 concat=True)

        op_expr = OpExpr(op_obj=groupby_agg,
                         op_on=self.groupby_from,
                         op_iter=True,
                         iter_on=self.groupby_from)

        self.groupby_from.push(op_expr)
        return self.groupby_from
