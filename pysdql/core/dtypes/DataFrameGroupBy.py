import inspect
import re

from pysdql.core.dtypes import AggrExpr, ColEl
from pysdql.core.dtypes.AggrLambdaExpr import AggrLambda
from pysdql.core.dtypes.DropDupOpExpr import DropDupOpExpr
from pysdql.core.dtypes.EnumUtil import AggrType, OpRetType
from pysdql.core.dtypes.GroupbyAggrExpr import GroupbyAggrExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.sdql_ir import ConstantExpr


class DataFrameGroupBy:
    def __init__(self, groupby_from, groupby_cols):
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols

    def agg(self, func=None, *agg_args, **agg_kwargs):
        if func:
            if type(func) == str:
                return self.agg_str_parse(func)
            if type(func) == dict:
                return self.agg_dict_parse(func)
        if agg_args:
            pass
        if agg_kwargs:
            return self.agg_kwargs_parse(agg_kwargs)

    def agg_str_parse(self, agg_str):
        pass

    def agg_dict_parse(self, input_aggr_dict):
        output_aggr_dict = {}
        tuple_aggr_dict = {}

        for k in input_aggr_dict.keys():
            tuple_aggr_dict[k] = (k, input_aggr_dict[k])

        for aggr_key in input_aggr_dict.keys():
            aggr_func = input_aggr_dict[aggr_key]

            if aggr_func == 'sum':
                output_aggr_dict[aggr_key] = self.groupby_from.key_access(aggr_key)
            if aggr_func == 'count':
                # i: int to float
                output_aggr_dict[aggr_key] = ConstantExpr(1.0)
            if aggr_func == 'min':
                output_aggr_dict[aggr_key] = self.groupby_from.key_access(aggr_key)

        groupby_agg = GroupbyAggrExpr(groupby_from=self.groupby_from,
                                      groupby_cols=self.groupby_cols,
                                      agg_dict=output_aggr_dict,
                                      concat=True,
                                      origin_dict=tuple_aggr_dict)

        op_expr = OpExpr(op_obj=groupby_agg,
                         op_on=self.groupby_from,
                         op_iter=True,
                         iter_on=self.groupby_from)

        self.groupby_from.push(op_expr)
        return self.groupby_from

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
                # i: int to float
                agg_dict[agg_key] = ConstantExpr(1.0)
            if agg_flag == 'min':
                agg_dict[agg_key] = agg_calc
            if callable(agg_flag):
                # received lambda function
                # i: int to float
                agg_dict[agg_key] = agg_flag(AggrLambda(self.groupby_from,
                                                        agg_tuple_dict[agg_key][0]))
            if agg_flag == 'mean':
                agg_dict[f'{agg_key}_sum_for_mean'] = agg_calc
                # i: int to float
                agg_dict[f'{agg_key}_count_for_mean'] = ConstantExpr(1.0)
            if agg_flag == 'last':
                self.groupby_cols.append(agg_key)

        groupby_agg = GroupbyAggrExpr(groupby_from=self.groupby_from,
                                      groupby_cols=self.groupby_cols,
                                      agg_dict=agg_dict,
                                      concat=True,
                                      origin_dict=agg_tuple_dict)

        op_expr = OpExpr(op_obj=groupby_agg,
                         op_on=self.groupby_from,
                         op_iter=True,
                         iter_on=self.groupby_from)

        self.groupby_from.push(op_expr)
        return self.groupby_from

    def __getitem__(self, item):
        return ColEl(self.groupby_from, item)

    def filter(self, func):
        cond = func(self)

        op_expr = OpExpr(op_obj=cond,
                         op_on=self.groupby_from,
                         op_iter=True,
                         iter_on=self.groupby_from)

        self.groupby_from.push(op_expr)

        return self.groupby_from

    def last(self):
        op_expr = OpExpr(op_obj=DropDupOpExpr(self.groupby_cols),
                         op_on=self.groupby_from,
                         op_iter=True,
                         iter_on=self.groupby_from)

        self.groupby_from.push(op_expr)

        return self.groupby_from
