from pysdql.core.dtypes.sdql_ir import RecConsExpr, RecAccessExpr


class GroupByAgg:
    def __init__(self, groupby_from, groupby_cols, agg_dict, concat: bool):
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols
        self.agg_dict = agg_dict
        self.concat = concat

    @property
    def aggr_keys(self):
        return RecConsExpr([(i, self.groupby_from.key_access(i)) for i in self.groupby_cols])

    def __repr__(self):
        return repr(self.agg_dict)

    @property
    def op_name_suffix(self):
        return f'_groupby_agg'
