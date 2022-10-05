class GroupByAgg:
    def __init__(self, groupby_from, groupby_cols, agg_dict, concat: bool):
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols
        self.agg_dict = agg_dict
        self.concat = concat

    @property
    def op_name_suffix(self):
        return f'_groupby_agg'

