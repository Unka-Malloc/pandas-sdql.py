from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import LastIterFunc
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.interfaces import Retrivable

from pysdql.core.dtypes import (
    ColEl,
    ColExpr,
    ColProjExpr,
    NewColOpExpr,
    OldColOpExpr,
    CondExpr,
    MergeExpr,
    AggrExpr,
    GroupbyAggrExpr,
    ExternalExpr,
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
        elif isinstance(expr_obj, CondExpr):
            cols += Retriever.find_cols(expr_obj.unit1)
            cols += Retriever.find_cols(expr_obj.unit2)
        elif isinstance(expr_obj, ExternalExpr):
            cols += Retriever.find_cols(expr_obj.col)
        return cols

    def find_cols_used(self, mode='', only_next=True):
        cols = []

        if mode == 'merge':
            if only_next:
                next_merges = self.findall_merge()
                for m in next_merges:
                    if m.joint.name == self.target.name:
                        continue

                    if isinstance(m.left_on, str) and isinstance(m.right_on, str):
                        cols.append(m.left_on)
                        cols.append(m.right_on)
                    if isinstance(m.left_on, list) and isinstance(m.right_on, list):
                        cols += m.left_on
                        cols += m.right_on
            else:
                raise NotImplementedError
        if mode == 'insert':
            for op_expr in self.history:
                op_body = op_expr.op

                # NewColOpExpr
                if isinstance(op_body, NewColOpExpr):
                    cols.append(op_body.col_var)
        else:
            raise NotImplementedError

        return cols

    def find_col_rename(self, col_name, by='val'):
        for op_expr in self.history:
            op_body = op_expr.op

            # OldColOpExpr
            if isinstance(op_body, OldColOpExpr):
                if by == 'key':
                    if op_body.col_var == col_name:
                        return op_body.col_expr
                if by == 'val':
                    if op_body.col_expr == col_name:
                        return op_body.col_var
        else:
            raise IndexError(f'Not found: {col_name}')

    def find_renamed_cols(self, mode='as_key'):
        cols_used = []
        cols_own = []

        cols_own += self.target.columns

        for op_expr in self.history:
            op_body = op_expr.op

            # OldColOpExpr
            if isinstance(op_body, OldColOpExpr):
                if mode == 'as_key':
                    if isinstance(op_body.col_var, str):
                        cols_used.append(op_body.col_var)
                    else:
                        TypeError('New Column: The names of new columns must be str.')

                if mode == 'as_val':
                    if isinstance(op_body.col_expr, str):
                        # If the key and value are both strings
                        # rename({'col_new', 'col_old'})
                        # both columns are owned by the joint one.
                        cols_own.append(op_body.col_var)
                        cols_used.append(op_body.col_expr)
                    elif isinstance(op_body.col_expr, (ColEl, ColExpr)):
                        cols_used += self.find_cols(op_body.col_expr)
                    elif isinstance(op_body.col_expr, Expr):
                        cols_used += SDQLInspector.find_cols(op_body.col_expr)
                    else:
                        raise NotImplementedError(f'Unsupport Type: {type(op_body.col_expr)}')

        return cols_used

    def findall_cols_used(self, as_owner=True, only_next=False) -> list:
        """

        :param only_next:
            True -> only find the columns that are used in the other joints rather than as the [left, right] side
            False -> fina all usages including the joint that construct itself
        :param as_owner:
            False -> find all columns from nested (joint) dataframe and include all columns (even not in current dataframe)
            True -> only find columns that the current dataframe has
        :return:
        """
        cols_used = []
        cols_own = []

        cols_own += self.target.columns

        for op_expr in self.history:
            op_body = op_expr.op

            # CondExpr
            if isinstance(op_body, CondExpr):
                if not only_next:
                    cols_used += self.find_cols(op_body)

            # NewColOpExpr
            if isinstance(op_body, NewColOpExpr):
                if isinstance(op_body.col_var, str):
                    cols_used.append(op_body.col_var)
                else:
                    TypeError('New Column: The names of new columns must be str.')

                if isinstance(op_body.col_expr, (ColEl, ColExpr, ExternalExpr)):
                    cols_used += self.find_cols(op_body.col_expr)
                elif isinstance(op_body.col_expr, Expr):
                    cols_used += SDQLInspector.find_cols(op_body.col_expr)
                else:
                    raise NotImplementedError(f'Unsupport Type: {type(op_body.col_expr)}')

            # OldColOpExpr
            if isinstance(op_body, OldColOpExpr):
                if isinstance(op_body.col_var, str):
                    cols_used.append(op_body.col_var)
                else:
                    TypeError('Old Column: The names of new columns must be str.')

                if isinstance(op_body.col_expr, str):
                    # If the key and value are both strings
                    # rename({'col_new', 'col_old'})
                    # both columns are owned by the joint one.
                    cols_own.append(op_body.col_var)
                    cols_used.append(op_body.col_expr)
                elif isinstance(op_body.col_expr, (ColEl, ColExpr)):
                    cols_used += self.find_cols(op_body.col_expr)
                elif isinstance(op_body.col_expr, Expr):
                    cols_used += SDQLInspector.find_cols(op_body.col_expr)
                else:
                    raise NotImplementedError(f'Unsupport Type: {type(op_body.col_expr)}')

            # MergeExpr
            if isinstance(op_body, MergeExpr):
                if not only_next:
                    if isinstance(op_body.left_on, str):
                        if isinstance(op_body.right_on, str):
                            cols_used.append(op_body.left_on)
                            cols_used.append(op_body.right_on)
                        else:
                            raise TypeError(
                                f'Type does not match: left_on {op_body.left_on} right_on {op_body.right_on}')
                    elif isinstance(op_body.left_on, list):
                        if isinstance(op_body.right_on, list):
                            cols_used += op_body.left_on
                            cols_used += op_body.right_on
                        else:
                            raise TypeError(
                                f'Type does not match: left_on {op_body.left_on} right_on {op_body.right_on}')
                    else:
                        raise TypeError('MergeExpr only accept list or str as left_on and right_on.')

                if self.target.name != op_body.joint.name:
                    cols_used += op_body.joint.get_retriever().findall_cols_used(as_owner=as_owner,
                                                                                 only_next=only_next)

            # GroupbyAgg
            if isinstance(op_body, GroupbyAggrExpr):
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

        if as_owner:
            return [x for x in cleaned_cols_used if x in cols_own]
        else:
            return cleaned_cols_used

    def find_dup_cols(self):
        dup_cols = []

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if self.target.name == op_body.joint.name:
                    dup_cols = [x for x in op_body.left.__columns
                                if x in op_body.right.__columns]

        # Remove Duplications
        cleaned_dup_cols = []
        [cleaned_dup_cols.append(x) for x in sorted(dup_cols) if x not in cleaned_dup_cols]

        return cleaned_dup_cols

    def find_illegal_dup_col(self):
        dup_cols = []

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if self.target.name == op_body.joint.name:
                    dup_cols = [x for x in op_body.left.__columns
                                if x in op_body.right.__columns
                                and x in self.findall_cols_used(as_owner=False)]

        # Remove Duplications
        cleaned_dup_cols = []
        [cleaned_dup_cols.append(x) for x in sorted(dup_cols) if x not in cleaned_dup_cols]

        return cleaned_dup_cols

    def find_col_ins_before(self, op_type) -> dict:
        col_ins = {}

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, NewColOpExpr):
                col_ins[op_body.col_var] = op_body.col_expr

            if isinstance(op_body, op_type):
                break

        return col_ins

    '''
    Operations
    '''

    @property
    def last_iter_is_merge(self):
        return isinstance(self.find_last_iter(), MergeExpr)

    @property
    def last_iter_is_groupby_agg(self):
        return isinstance(self.find_last_iter(), GroupbyAggrExpr)

    @property
    def last_iter_is_agg(self):
        return isinstance(self.find_last_iter(), AggrExpr)

    def find_last_iter(self, body_only=True, as_enum=False):
        for op_expr in reversed(self.history):
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if as_enum:
                    return LastIterFunc.Joint
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, AggrExpr):
                if as_enum:
                    return LastIterFunc.Agg
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, GroupbyAggrExpr):
                if as_enum:
                    return LastIterFunc.GroupbyAgg
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, CalcExpr):
                if as_enum:
                    return LastIterFunc.Calc
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

    @staticmethod
    def split_cond(cond_expr: CondExpr):
        return cond_expr.unit1, cond_expr.unit2

    @staticmethod
    def replace_cond(cond: CondExpr, mapper: dict) -> CondExpr:
        new_unit1 = cond.unit1
        new_unit2 = cond.unit2

        if isinstance(cond.unit1, CondExpr):
            new_unit1 = Retriever.replace_cond(cond.unit1, mapper)
        if isinstance(cond.unit2, CondExpr):
            new_unit2 = Retriever.replace_cond(cond.unit2, mapper)

        if isinstance(cond.unit1, ColEl):
            col_name = cond.unit1.field

            if col_name in mapper.keys():
                new_unit1 = mapper[col_name]

        if isinstance(cond.unit2, ColEl):
            col_name = cond.unit2.field

            if col_name in mapper.keys():
                new_unit2 = mapper[col_name]

        result = CondExpr(new_unit1,
                          cond.op,
                          new_unit2)

        return result

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

    def find_cond_before(self, op_type, body_only=True):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CondExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, op_type):
                return None
        else:
            return None

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

                if self.target.name != op_body.joint.name:
                    all_merges += op_body.joint.get_retriever().findall_merge()

        # Remove Duplications
        cleaned_all_merges = []
        [cleaned_all_merges.append(x) for x in all_merges if x not in cleaned_all_merges]

        return cleaned_all_merges

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
    def as_probe_for_next_join(self):
        for op_expr in reversed(self.history):
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if self.target.name == op_expr.op.right.name:
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

    @property
    def is_joint(self) -> bool:
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    return True
        else:
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

    def find_root_probe(self):
        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    if op_body.right.get_retriever().is_joint:
                        return op_body.right.get_retriever().find_root_probe()
                    else:
                        return op_body.right
        else:
            raise ValueError('Cannot find the root probe side.')

    def finall_key_for_root_probe(self):
        keys = []

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    keys.append(op_body.right_on)

                    if op_body.right.get_retriever().is_joint:
                        keys += op_body.right.get_retriever().finall_key_for_root_probe()

        return keys

    def find_probe_key_as_part_side(self):
        """
        Find the probe key for the probe side based on the part side.
        :param part_side:
        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.left.name == self.target.name:
                    return op_body.right_on
        else:
            raise IndexError('Cannot find a merge.')

    def findall_part_for_root_probe(self, mode=''):
        parts = []

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    if mode == 'as_body':
                        parts.append(op_body.left)
                    if mode == 'as_frame':
                        parts.append(op_body.left.get_part_frame())
                    if mode == 'as_expr':
                        if op_body.left.get_retriever().is_joint:
                            parts.append(op_body.left.get_joint_frame().get_joint_expr())
                        else:
                            parts.append(op_body.left.get_part_frame().get_part_expr())

                    if op_body.right.get_retriever().is_joint:
                        parts += op_body.right.get_retriever().findall_part_for_root_probe(mode)

        return parts

    '''
    GroupbyAgg
    '''

    def find_groupby_agg(self, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, GroupbyAggrExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr
        else:
            return None

    '''
    Aggregation
    '''

    def find_agg(self, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AggrExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr
        else:
            return None

    '''
    isin()
    '''
    def find_isin(self, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, IsInExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr
        else:
            return None

    def find_isin_before(self, op_type, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, IsInExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, op_type):
                return None
        else:
            return None
