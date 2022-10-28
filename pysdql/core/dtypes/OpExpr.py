class OpExpr:
    def __init__(self, op_obj, op_on, op_iter: bool, iter_on=None, ret_type=None):
        # self.op_info = op_info
        # self.op_obj = op_obj

        # self.data = (self.op_info, self.op_obj)

        self.__info_iter = op_iter
        self.__info_iter_on = iter_on
        self.__info_op = op_obj
        self.__info_op_type = type(op_obj)
        self.__info_on = op_on
        self.__info_ret_type = ret_type

    def get_op_name_suffix(self):
        return self.op.op_name_suffix

    def get_op_name(self) -> str:
        op_name = f'{self.op_on.name}'
        op_name += self.op.op_name_suffix

        return op_name

    @property
    def name(self):
        if self.__info_iter:
            return self.get_op_name()
        else:
            return None

    @property
    def iter(self):
        # enum([None, 'scalar', 'dict', 'record', 'groupby_dict'])
        return self.__info_iter

    @iter.setter
    def iter(self, val):
        if val not in (True, False):
            raise ValueError()
        self.__info_iter = val

    @property
    def iter_on(self):
        return self.__info_iter_on

    @property
    def op(self):
        return self.__info_op

    @op.setter
    def op(self, val):
        self.__info_op = val

    @property
    def op_type(self):
        # enum([CondExpr, ])
        return self.__info_op_type

    @op_type.setter
    def op_type(self, val):
        self.__info_op_type = val

    @property
    def op_on(self):
        # enum([None, DataFrame(), ColEl(), ColExpr()])
        return self.__info_on

    @op_on.setter
    def op_on(self, val):
        self.__info_on = val

    @property
    def ret_type(self):
        return self.__info_ret_type

    @ret_type.setter
    def ret_type(self, val):
        self.__info_ret_type = val

    @property
    def info(self):
        return {
            'iter': self.iter,
            'op_type': self.op_type,
            'op': self.op,
            'ret_type': self.ret_type,
            'on': self.op_on,
        }

    # @property
    # def op_ref_dict(self):
    #     return {'relation_data': 0,
    #             'relation_selection': 1,
    #             'groupby_nested_dict': 2,
    #             'groupby_grouped_dict': 3,
    #             'groupby_aggregate_parse_nested_dict': 4,
    #             'groupby_aggregate_result': 5,
    #             'relation_aggregate_kwargs_aggr_tuple': 6,
    #             'relation_aggregate_kwargs_aggr_result': 7,
    #             'pysdql_merge_by_cols': 8,
    #             'pysdql_merge_on_by_cols': 9,
    #             'relation_exists': 10,
    #             'relation_not_exists': 11,
    #             'colexpr_aggr_sum': 12,
    #             'grouby_filter_output': 13,
    #             'havepxr_hvmp': 14,
    #             'havexpr_hvr': 15,
    #             'havexpr_fhvr': 16,
    #             'relation_rename': 17,
    #             'relation_set_caseexpr': 18,
    #             'pysdql_merge_on_by_concatexpr': 19,
    #             'pysdql_merge_by_concatexpr': 20,
    #             'varexpr_rename': 21,
    #             'relation_projection': 22,
    #             'relation_selection_isin': 23,
    #             'relation_load_data': 24,
    #             'relation_merge_on_by_concatexpr': 25,
    #             'relation_merge_by_concatexpr': 26,
    #             'relation_count': 27,
    #             'relation_optimized_merge_part_right': 28,
    #             'relation_optimized_merge_result': 29,
    #             'relation_rename_col': 30,
    #             'relation_head': 31,
    #             'relation_selection_ext': 32,
    #             'relation_setitem_list_loopfusion': 33,
    #             'groupby_optimized_agg_parse_nested_dict': 34,
    #             'groupby_optimized_agg_result': 35,
    #             'relation_agg_dict_agg_tuple': 36,
    #             'relation_agg_dict_agg_result_value': 37,
    #             'relation_selection_not_exists_groupby': 38,
    #             'relation_selection_not_exists_output': 39,
    #             'relation_selection_isin_invert': 40,
    #             'relation_drop_duplicates_1st': 41,
    #             'relation_drop_duplicates_2nd': 42,
    #             'joinexpr_get_left_joint_dict_right_group_dict': 43,
    #             'joinexpr_get_left_joint_dict_left_group_dict': 44,
    #             'colel_sum': 45,
    #             }

    # @property
    # def opuid(self):
    #     ref_id = self.op_ref_dict[self.op_info]
    #     return hash((ref_id, self.op_obj))
    #

    #
    # @property
    # def expr(self):
    #     return f'{self.op_obj}'

    def __repr__(self):
        return repr(self.info)
    #
    # def __hash__(self):
    #     return hash((self.op_info, self.op_obj))
    #
    # def __eq__(self, other):
    #     if type(other) == OpExpr:
    #         if (self.op_info, self.op_obj) == (other.op_info, other.op_obj):
    #             return True
    #     return False
