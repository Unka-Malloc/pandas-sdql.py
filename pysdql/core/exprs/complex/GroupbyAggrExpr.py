from typing import List

from pysdql.core.interfaces.availability.Replaceable import Replaceable


class GroupbyAggrExpr(Replaceable):
    def __init__(self, groupby_from, groupby_cols: List[str], agg_dict: dict, concat: bool, origin_dict=None):
        self.groupby_from = groupby_from
        self.groupby_cols = groupby_cols
        self.groupby_keys = groupby_cols
        self.aggr_dict = agg_dict
        self.origin_dict = origin_dict
        self.concat = concat

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return hash((
            self.groupby_from.name,
            tuple(self.groupby_keys),
            tuple(self.origin_dict.items())
        ))

    @property
    def sdql_ir(self):
        return

    def __repr__(self):
        return repr(self.aggr_dict)

    @property
    def op_name_suffix(self):
        return f'_groupby_agg'
