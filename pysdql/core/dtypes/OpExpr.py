class OpExpr:
    def __init__(self, op_str, op_obj):
        self.op_str = op_str
        self.op_obj = op_obj

        self.data = (self.op_str, self.op_obj)

    @property
    def op_ref_dict(self):
        return {'relation_data': 0,
                'relation_selection': 1,
                'groupby_nested_dict': 2,
                'groupby_result': 3,
                'groupby_aggr_parse_nested_dict': 4,
                'groupby_aggr_result': 5,
                'relation_aggr_kwargs_aggr_tuple': 6,
                'relation_aggr_kwargs_aggr_result': 7,
                'pysdql_merge': 8,
                'pysdql_merge_on': 9,
                'relation_exists': 10,
                'relation_not_exists': 11,
                'colexpr_aggr_sum': 12,
                'grouby_filter_output': 13
                }

    @property
    def expr(self):
        return f'{self.op_obj}'

    def __repr__(self):
        return self.expr

    def __hash__(self):
        return hash((self.op_str, self.op_obj))

    def __eq__(self, other):
        if type(other) == OpExpr:
            if (self.op_str, self.op_obj) == (other.op_str, other.op_obj):
                return True
        return False
