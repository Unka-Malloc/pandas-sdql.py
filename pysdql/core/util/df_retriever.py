from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.interfaces import Retrivable

from pysdql.core.dtypes import (
    CondExpr,
    MergeExpr,
    GroupByAgg,
    AggrExpr,
    VirColExpr,
    ColEl,
    ColExpr,
    ColProjExpr,
)

from pysdql.core.dtypes.sdql_ir import *


class Retriever:
    def __init__(self, target: Retrivable):
        """

        :param DataFrame target:
        """
        self.target = target

    @property
    def history(self):
        return self.target.get_history()

    '''
    Columns
    '''

    @staticmethod
    def find_cols(expr_obj) -> list:
        cols = []
        if isinstance(expr_obj, ColEl):
            cols.append(expr_obj.field)
        elif isinstance(expr_obj, ColExpr):
            cols += Retriever.find_cols(expr_obj.unit1)
            cols += Retriever.find_cols(expr_obj.unit2)

        return cols

    def findall_cols_used(self, nested=False) -> list:
        """

        :param nested:
            True -> find all columns from nested (joint) dataframe and include all columns (even not in current dataframe)
            False -> only find columns that the current dataframe has
        :return:
        """
        cols_used = []

        for op_expr in self.history:
            op_body = op_expr.op

            # MergeExpr
            if isinstance(op_body, MergeExpr):
                if isinstance(op_body.left_on, str):
                    if isinstance(op_body.right_on, str):
                        cols_used.append(op_body.left_on)
                        cols_used.append(op_body.right_on)
                    else:
                        raise TypeError(f'Type does not match: left_on {op_body.left_on} right_on {op_body.right_on}')
                elif isinstance(op_body.left_on, list):
                    if isinstance(op_body.right_on, list):
                        cols_used += op_body.left_on
                        cols_used += op_body.right_on
                    else:
                        raise TypeError(f'Type does not match: left_on {op_body.left_on} right_on {op_body.right_on}')
                else:
                    raise TypeError('MergeExpr only accept list or str as left_on and right_on.')

                if self.target.name == op_body.left.name or self.target.name == op_body.right.name:
                    cols_used += op_body.joint.get_retriever().findall_cols_used()

            # VirColEl
            if isinstance(op_body, VirColExpr):
                if isinstance(op_body.col_var, str):
                    cols_used.append(op_body.col_var)
                else:
                    TypeError('Virtual Column: The names of virtual columns must be str.')
                if isinstance(op_body.col_expr, ColExpr):
                    cols_used += self.find_cols(op_body.col_expr)
                elif isinstance(op_body.col_expr, Expr):
                    cols_used += SDQLInspector.find_cols(op_body.col_expr)
                else:
                    raise NotImplementedError

            # GroupbyAgg
            if isinstance(op_body, GroupByAgg):
                cols_used += op_body.groupby_cols

                if isinstance(op_body.origin_dict, dict):
                    for k in op_body.origin_dict.keys():
                        v = op_body.origin_dict[k]
                        if isinstance(k, str):
                            cols_used.append(k)
                        if isinstance(v, tuple):
                            cols_used.append(v[0])
                        else:
                            raise NotImplementedError
                else:
                    raise TypeError('Groupby aggregation dictionary must be dict.')

        # Remove Duplications
        cleaned_cols_used = []
        [cleaned_cols_used.append(x) for x in sorted(cols_used) if x not in cleaned_cols_used]

        if nested:
            return cleaned_cols_used
        else:
            return [x for x in cleaned_cols_used if x in self.target.columns]

    '''
    Operations
    '''

    @property
    def last_iter_is_merge(self):
        return isinstance(self.find_last_iter(), MergeExpr)

    @property
    def last_iter_is_groupby_agg(self):
        return isinstance(self.find_last_iter(), GroupByAgg)

    @property
    def last_iter_is_agg(self):
        return isinstance(self.find_last_iter(), AggrExpr)

    def find_last_iter(self, body_only=True):
        for op_expr in reversed(self.history):
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, AggrExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, GroupByAgg):
                if body_only:
                    return op_body
                else:
                    return op_expr

    def find_last_op(self, body_only=True):
        op_expr = self.history.peak()
        if body_only:
            return op_expr.op
        else:
            return op_expr

    def find_ops_except(self, black_list: list, body_only=True):
        ops = []
        for op_expr in self.history:
            op_body = op_expr.op

            if not any(isinstance(op_body, t) for t in black_list):
                if body_only:
                    ops.append(op_body)
                else:
                    ops.append(op_expr)
        return ops

    '''
    CondExpr
    '''

    def findall_cond(self, body_only=True):
        """
        It returns a list that contains all conditions (CondExpr objects) in the history operations.
        :return:
        """
        all_conds = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CondExpr):
                if body_only:
                    all_conds.append(op_body)
                else:
                    all_conds.append(op_expr)

        return all_conds

    '''
    MergeExpr
    '''

    def findall_merge(self, body_only=True) -> list:
        """
        It returns a list that contains all MergeExpr objects in the history operations.
        :return:
        """
        all_merges = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if body_only:
                    all_merges.append(op_body)
                else:
                    all_merges.append(op_expr)

        return all_merges

    def find_merge(self, mode: str):
        """

        :param mode: ['as_part', 'as_probe', 'as_joint']
        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if mode == 'as_joint':
                    if self.target.name == op_body.joint.name:
                        return op_body
                elif mode == 'as_part':
                    if self.target.name == op_body.left.name:
                        return op_body
                elif mode == 'as_probe':
                    if self.target.name == op_body.right.name:
                        return op_body

        return None

    def find_merge_side(self, mode: str):
        """

        :param mode: ['part_side', 'probe_side', 'joint_side']
        :return:
        """

        return

    def find_latest_merge(self, body_only=True):
        for op_expr in reversed(self.history):
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

        return None

    @property
    def as_part_for_next_join(self):
        for op_expr in reversed(self.history):
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if self.target.name == op_expr.op.left.name:
                    return True
        return False

    @property
    def as_bypass_for_next_join(self):
        """
        Try to find a column that is used in the following operations except MergeExpr.
        If failed, then this dataframe is completely a bypass dataframe for next join,
            there will be no partition (no iteration).
        Otherwise, the partition must occurs (must be an separate iteration)
        :return:
        """
        if self.as_part_for_next_join:
            if not self.find_ops_except([ColProjExpr, MergeExpr]):
                cols_for_merge = self.find_merge(mode='as_part').left_on
                cols_for_use = self.findall_cols_used()
                cols_all = self.target.columns
                for c in cols_for_use:
                    if c in cols_all:
                        '''
                        If a column is not for merge, it must be used somewhere else, 
                        therefore, this partition cannot be a bypass partition side
                        '''
                        if isinstance(cols_for_merge, str):
                            if c != cols_for_merge:
                                return False
                        elif isinstance(cols_for_merge, list):
                            if c not in cols_for_merge:
                                return False
                else:
                    return True
        return False

    def is_last_joint(self) -> bool:
        is_the_joint = False

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):

                # not_as_part_side
                if op_body.left.name == self.target.name:
                    return False

                # not_as_probe_side
                if op_body.right.name == self.target.name:
                    return False

                # is_the_joint
                if op_body.joint.name == self.target.name:
                    is_the_joint = True

        return is_the_joint

    '''
    GroupbyAgg
    '''

    def findall_groupby_agg(self, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """
        all_groupby_agg = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, GroupByAgg):
                if body_only:
                    all_groupby_agg.append(op_body)
                else:
                    all_groupby_agg.append(op_expr)

        return all_groupby_agg
