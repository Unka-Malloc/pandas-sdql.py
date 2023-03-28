from pysdql.core.dtypes.AddColProj import AddColProj
from pysdql.core.dtypes.ApplyOpExprUnopt import ApplyOpExprUnopt
from pysdql.core.dtypes.ColApplyExpr import ColApplyExpr
from pysdql.core.dtypes.FreeStateVarDefExpr import FreeStateVar
from pysdql.core.dtypes.NewColListExpr import NewColListExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.AggrFiltCond import AggrFiltCond
from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.EnumUtil import LastIterFunc, OpRetType
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.interfaces import Retrivable

from pysdql.core.dtypes import (
    ColEl,
    ColOpExpr,
    ColProjExpr,
    NewColOpExpr,
    OldColOpExpr,
    CondExpr,
    MergeExpr,
    AggrExpr,
    GroupbyAggrExpr,
    ColExtExpr,
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
    def find_cols(expr_obj, as_expr=False) -> list:
        cols = []
        if isinstance(expr_obj, ColEl):
            if as_expr:
                cols.append(expr_obj)
            else:
                cols.append(expr_obj.field)
        elif isinstance(expr_obj, ColOpExpr):
            cols += Retriever.find_cols(expr_obj.unit1, as_expr)
            cols += Retriever.find_cols(expr_obj.unit2, as_expr)
        elif isinstance(expr_obj, CondExpr):
            cols += Retriever.find_cols(expr_obj.unit1, as_expr)
            cols += Retriever.find_cols(expr_obj.unit2, as_expr)
        elif isinstance(expr_obj, ColExtExpr):
            cols += Retriever.find_cols(expr_obj.col, as_expr)
        elif isinstance(expr_obj, GroupbyAggrExpr):
            cols += expr_obj.groupby_cols
            if isinstance(list(expr_obj.origin_dict.values())[0], tuple):
                for v in expr_obj.origin_dict.values():
                    if as_expr:
                        cols.append(expr_obj.groupby_from.get_col(v[0]))
                    else:
                        cols.append(v[0])
            else:
                raise NotImplementedError
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
        elif mode == 'probe':
            if only_next:
                next_merges = self.findall_merge()
                for m in next_merges:
                    if m.joint.name == self.target.name:
                        continue

                    if isinstance(m.left_on, str) and isinstance(m.right_on, str):
                        cols.append(m.right_on)
                    if isinstance(m.left_on, list) and isinstance(m.right_on, list):
                        cols += m.right_on
            else:
                raise NotImplementedError
        elif mode == 'part':
            if only_next:
                next_merges = self.findall_merge()
                for m in next_merges:
                    if m.joint.name == self.target.name:
                        continue

                    if isinstance(m.left_on, str) and isinstance(m.right_on, str):
                        cols.append(m.left_on)
                    if isinstance(m.left_on, list) and isinstance(m.right_on, list):
                        cols += m.left_on
            else:
                raise NotImplementedError
        elif mode == 'insert':
            for op_expr in self.history:
                op_body = op_expr.op

                # NewColOpExpr
                if isinstance(op_body, NewColOpExpr):
                    cols.append(op_body.col_var)
        elif mode == 'groupby_aggr':
            for op_expr in self.history:
                op_body = op_expr.op

                # GroupbyAggrExpr
                if isinstance(op_body, GroupbyAggrExpr):
                    cols += op_body.groupby_cols
                    cols += list(op_body.origin_dict.keys())

        elif mode == 'aggr':
            for op_expr in self.history:
                op_body = op_expr.op

                # AggrExpr
                if isinstance(op_body, AggrExpr):
                    cols += list(op_body.aggr_op.keys())
        elif mode == 'aggregation':
            for op_expr in self.history:
                op_body = op_expr.op

                # GroupbyAggrExpr
                if isinstance(op_body, GroupbyAggrExpr):
                    cols += op_body.groupby_cols
                    cols += list(op_body.origin_dict.keys())

                # AggrExpr
                if isinstance(op_body, AggrExpr):
                    cols += list(op_body.aggr_op.keys())
        else:
            raise NotImplementedError

        return cols

    def findall_col_rename(self, reverse=False) -> dict:
        result = {}
        for op_expr in self.history:
            op_body = op_expr.op

            # OldColOpExpr
            if isinstance(op_body, OldColOpExpr):
                if reverse:
                    result[op_body.col_expr] = op_body.col_var
                else:
                    result[op_body.col_var] = op_body.col_expr
        else:
            return result

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
                    elif isinstance(op_body.col_expr, (ColEl, ColOpExpr)):
                        cols_used += self.find_cols(op_body.col_expr)
                    elif isinstance(op_body.col_expr, Expr):
                        cols_used += SDQLInspector.find_cols(op_body.col_expr)
                    else:
                        raise NotImplementedError(f'Unsupport Type: {type(op_body.col_expr)}')

        return cols_used

    def findall_cols_for_groupby_aggr(self, as_owner=True, only_next=False) -> list:
        cols_used = []
        cols_own = []

        cols_own += self.target.columns

        for op_expr in self.history:
            op_body = op_expr.op

            # MergeExpr
            if isinstance(op_body, MergeExpr):
                if self.target.name != op_body.joint.name:
                    cols_used += op_body.joint.get_retriever().findall_cols_for_groupby_aggr(as_owner=as_owner,
                                                                                             only_next=only_next)

            # GroupbyAgg
            if isinstance(op_body, GroupbyAggrExpr):
                cols_used += op_body.groupby_cols

                if isinstance(op_body.origin_dict, dict):
                    for k in op_body.origin_dict.keys():
                        v = op_body.origin_dict[k]
                        # {k : v}
                        if isinstance(v, str):
                            cols_used.append(k)
                        # {k : (v0, v1)}
                        elif isinstance(v, tuple):
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

                if isinstance(op_body.col_expr, (ColEl, ColOpExpr, ColExtExpr)):
                    cols_used += self.find_cols(op_body.col_expr)
                elif isinstance(op_body.col_expr, Expr):
                    cols_used += SDQLInspector.find_cols(op_body.col_expr)
                elif isinstance(op_body.col_expr, ColApplyExpr):
                    cols_used += SDQLInspector.find_cols(op_body.col_expr.sdql_ir)
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
                elif isinstance(op_body.col_expr, (ColEl, ColOpExpr)):
                    cols_used += self.find_cols(op_body.col_expr)
                elif isinstance(op_body.col_expr, ColApplyExpr):
                    cols_used += SDQLInspector.find_cols(op_body.col_expr.sdql_ir)
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

            # Isin
            if isinstance(op_body, IsInExpr):
                cols_used.append(op_body.col_part.col_name)
                cols_used.append(op_body.col_probe.col_name)

        # Remove Duplications
        cleaned_cols_used = []
        [cleaned_cols_used.append(x) for x in sorted(cols_used) if x not in cleaned_cols_used]

        if as_owner:
            return [x for x in cleaned_cols_used if x in cols_own]
        else:
            return cleaned_cols_used

    def find_cols_probed(self, as_owner=True, only_next=True):
        # Remove Duplications
        cleaned_cols_used = []
        [cleaned_cols_used.append(x) for x in self.find_cols_used(mode='probe') if x not in cleaned_cols_used]

        if as_owner:
            return [x for x in cleaned_cols_used if x in self.target.columns]
        else:
            return cleaned_cols_used

    def find_dup_cols(self):
        dup_cols = []

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if self.target.name == op_body.joint.name:
                    dup_cols = [x for x in op_body.left.columns
                                if x in op_body.right.columns]

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
                    dup_cols = [x for x in op_body.left.columns
                                if x in op_body.right.columns
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

    def find_col_ins_after(self, op_type) -> dict:
        col_ins = {}

        target_located = False
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, op_type):
                target_located = True

            if target_located:
                if isinstance(op_body, NewColOpExpr):
                    col_ins[op_body.col_var] = op_body.col_expr
        else:
            return col_ins

    def findall_col_insert(self) -> dict:
        col_ins = {}

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, NewColOpExpr):
                col_ins[op_body.col_var] = op_body.col_expr

        return col_ins

    def findall_col_insert_as_list(self) -> dict:
        col_ins = {}

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, NewColListExpr):
                col_ins[op_body.col_var] = op_body.col_list

        return col_ins

    '''
    Operations
    '''

    @property
    def last_iter_is_merge(self):
        return isinstance(self.find_last_iter(), MergeExpr)

    @property
    def last_iter_is_groupby_aggr(self):
        return isinstance(self.find_last_iter(), GroupbyAggrExpr)

    @property
    def last_iter_is_aggr(self):
        return isinstance(self.find_last_iter(), AggrExpr)

    @property
    def last_iter_is_calc(self):
        return isinstance(self.find_last_iter(), CalcExpr)

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
        op_expr = self.history.peek()
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
    def split_cond(cond: CondExpr, mapper: dict) -> dict:
        on = {}

        return on

    @staticmethod
    def find_cond_on(cond: CondExpr, mapper: dict, as_expr=False) -> list:
        on = []

        if isinstance(cond.unit1, ColEl):
            col_name = cond.unit1.field
            for n in mapper.keys():
                if col_name in mapper[n]:
                    on.append(n)
        elif isinstance(cond.unit1, ColOpExpr):
            for col_name in Retriever.find_cols(cond.unit1):
                for n in mapper.keys():
                    if col_name in mapper[n]:
                        on.append(n)
        elif isinstance(cond.unit1, ColExtExpr):
            col_name = cond.unit1.col.field
            for n in mapper.keys():
                if col_name in mapper[n]:
                    on.append(n)
        elif isinstance(cond.unit1, CondExpr):
            on += Retriever.find_cond_on(cond.unit1, mapper, as_expr)
        elif isinstance(cond.unit1, (ConstantExpr, VarExpr)):
            pass
        elif isinstance(cond.unit1, (bool, int, float, str)):
            pass
        else:
            raise ValueError(f'Unexpected unit1 type {type(cond.unit1)}')

        if isinstance(cond.unit2, ColEl):
            col_name = cond.unit2.field
            for n in mapper.keys():
                if col_name in mapper[n]:
                    on.append(n)
        elif isinstance(cond.unit2, ColOpExpr):
            for col_name in Retriever.find_cols(cond.unit2):
                for n in mapper.keys():
                    if col_name in mapper[n]:
                        on.append(n)
        elif isinstance(cond.unit2, ColExtExpr):
            col_name = cond.unit2.col.field
            for n in mapper.keys():
                if col_name in mapper[n]:
                    on.append(n)
        elif isinstance(cond.unit2, CondExpr):
            on += Retriever.find_cond_on(cond.unit2, mapper, as_expr)
        elif isinstance(cond.unit2, (ConstantExpr, VarExpr)):
            pass
        elif isinstance(cond.unit2, (bool, int, float, str)):
            pass
        else:
            raise ValueError(f'Unexpected unit2 type {type(cond.unit2)}')

        cleaned_on = []
        [cleaned_on.append(x) for x in on if x not in cleaned_on]

        return cleaned_on

    @staticmethod
    def findall_cols_in_cond(cond, as_expr=False):
        all_cols = []

        if isinstance(cond.unit1, ColEl):
            if as_expr:
                all_cols.append(cond.unit1)
            else:
                all_cols.append(cond.unit1.field)
        elif isinstance(cond.unit1, (ColOpExpr, ColExtExpr)):
            all_cols += Retriever.find_cols(cond.unit1, as_expr)
        elif isinstance(cond.unit1, CondExpr):
            all_cols += Retriever.findall_cols_in_cond(cond.unit1, as_expr)
        elif isinstance(cond.unit1, (ConstantExpr, VarExpr)):
            pass
        elif isinstance(cond.unit1, (bool, int, float, str)):
            pass
        else:
            raise ValueError(f'Unexpected unit1 type {type(cond.unit1)}')

        if isinstance(cond.unit2, ColEl):
            if as_expr:
                all_cols.append(cond.unit2)
            else:
                all_cols.append(cond.unit2.field)
        elif isinstance(cond.unit2, (ColOpExpr, ColExtExpr)):
            all_cols += Retriever.find_cols(cond.unit2, as_expr)
        elif isinstance(cond.unit2, CondExpr):
            all_cols += Retriever.findall_cols_in_cond(cond.unit2, as_expr)
        elif isinstance(cond.unit2, (ConstantExpr, VarExpr)):
            pass
        elif isinstance(cond.unit2, (bool, int, float, str)):
            pass
        else:
            raise ValueError(f'Unexpected unit2 type {type(cond.unit2)}')

        cleaned_cols = []
        if as_expr:
            for x in all_cols:
                if all([x.oid != i.oid for i in cleaned_cols]):
                    cleaned_cols.append(x)
        else:
            [cleaned_cols.append(x) for x in all_cols if x not in cleaned_cols]

        return cleaned_cols

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

    def find_cond(self, body_only=True):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CondExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, ColExtExpr):
                if op_body.func in [ExtFuncSymbol.StartsWith,
                                    ExtFuncSymbol.EndsWith,
                                    ExtFuncSymbol.StringContains]:
                    if body_only:
                        return op_body
                    else:
                        return op_expr
        else:
            return None

    def find_cond_before(self, op_type, body_only=True):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CondExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr

            if isinstance(op_body, ColExtExpr):
                if op_body.func in [ExtFuncSymbol.StartsWith,
                                    ExtFuncSymbol.EndsWith,
                                    ExtFuncSymbol.StringContains]:
                    if body_only:
                        return op_body
                    else:
                        return op_expr

            if isinstance(op_body, op_type):
                return None
        else:
            return None

    def find_cond_after(self, op_type, body_only=True):
        target_located = False
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, op_type):
                target_located = True

            if isinstance(op_body, CondExpr):
                if target_located:
                    if body_only:
                        return op_body
                    else:
                        return op_expr
        else:
            return None

    '''
    MergeExpr
    '''

    def findall_merge(self, body_only=True, only_next=True) -> list:
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

                if only_next:
                    if self.target.name != op_body.joint.name:
                        all_merges += op_body.joint.get_retriever().findall_merge(body_only, only_next)

        # Remove Duplications
        cleaned_all_merges = []
        [cleaned_all_merges.append(x) for x in all_merges if x not in cleaned_all_merges]

        return cleaned_all_merges

    def find_merge(self, mode='') -> MergeExpr:
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
                else:
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
    def as_aggr_for_next_join(self):
        next_merge = self.find_merge('as_part')
        if next_merge.joint.retriever.findall_groupby_aggr(drop_last=True):
            groupby_aggr_expr = next_merge.joint.retriever.findall_groupby_aggr()[0]
            if next_merge.joint.retriever.has_multi_gourpby_aggr:
                if all([i in self.target.columns or i == next_merge.right_on
                        for i in self.find_cols(groupby_aggr_expr)]):
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

    def find_root_merge(self):
        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    if op_body.right.get_retriever().is_joint:
                        return op_body.right.get_retriever().find_root_merge()
                    else:
                        return op_body
        else:
            raise ValueError('Cannot find the root probe side.')

    def find_root_part(self):
        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    if op_body.right.get_retriever().is_joint:
                        return op_body.right.get_retriever().find_root_part()
                    else:
                        return op_body.left
        else:
            raise ValueError('Cannot find the root probe side.')
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

    def find_root_probe_key(self):
        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    if op_body.right.retriever.is_joint:
                        return op_body.right.retriever.find_root_probe_key()
                    else:
                        return op_body.right_on
        else:
            raise ValueError('Cannot find the root probe side.')

    def findall_key_for_root_probe(self, as_tuple=False, ret_type=list):
        keys_list = []
        keys_dict = {}

        for op_expr in self.history:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    if as_tuple:
                        keys_dict[(op_body.left_on, op_body.right_on)] = op_body
                        keys_list.append((op_body.left_on, op_body.right_on))
                    else:
                        keys_dict[op_body.right_on] = op_body
                        keys_list.append(op_body.right_on)

                    if op_body.right.retriever.is_joint:
                        next_dict = op_body.right.retriever.findall_key_for_root_probe(as_tuple, ret_type=dict)
                        for k in next_dict.keys():
                            keys_dict[k] = next_dict[k]

                        keys_list += op_body.right.get_retriever().findall_key_for_root_probe(as_tuple, ret_type)

        if ret_type == list:
            return keys_list
        if ret_type == dict:
            return keys_dict

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
                            for i in SDQLInspector.split_bindings(op_body.left.get_joint_frame().get_joint_expr()):
                                parts.append(i)
                        else:
                            parts.append(op_body.left.get_part_frame().get_part_expr())

                    if op_body.right.get_retriever().is_joint:
                        for i in op_body.right.get_retriever().findall_part_for_root_probe(mode):
                            parts.append(i)

        if mode == 'as_expr':
            return SDQLInspector.remove_dup_bindings(parts)

        return parts

    @staticmethod
    def find_lookup_path(start_from, key_to: str):
        """
        nation left -> right customer_orders_join -> customer left -> right orders
        :param start_from: 
        :param key_to:
        :return:
        """

        root_merge = start_from.retriever.find_root_merge()
        root_part = root_merge.left
        root_probe = root_merge.right
        root_part_key = root_merge.left_on
        root_probe_key = root_merge.right_on

        all_parts = start_from.retriever.findall_part_for_root_probe('as_body')

        if isinstance(root_part_key, list) and isinstance(root_probe_key, list):
            if key_to in root_part_key:
                lookup_expr = root_probe.key_access(root_probe_key[root_part_key.index(key_to)])

                # print(f'column {key_to} is in {root_part}, can be accessed by {lookup_expr}')

                return lookup_expr
            elif key_to in root_probe_key:
                lookup_expr = root_probe.key_access(key_to)

                # print(f'column {key_to} is in {root_probe}, can be accessed by {lookup_expr}')

                return lookup_expr
            elif key_to in root_probe.columns:
                lookup_expr = root_probe.key_access(key_to)

                # print(f'column {key_to} is in {root_probe}, can be accessed by {lookup_expr}')

                return lookup_expr
            elif key_to in root_part.columns:
                lookup_expr = RecAccessExpr(recExpr=DicLookupExpr(dicExpr=root_part.var_part,
                                                                  keyExpr=RecConsExpr([(root_part_key[root_probe_key.index(i)],
                                                                                        root_probe.key_access(i))
                                                                                       for i in root_probe_key])),
                                            fieldName=key_to)

                # print(f'column {key_to} is in {root_part}, can be accessed by {lookup_expr}')

                return lookup_expr
            else:
                for part in all_parts:
                    if key_to in part.columns:
                        lookup_expr = Retriever.find_bypass_lookup(all_parts, key_to, root_merge)

                        # print(f'column {key_to} is in {root_part}, can be accessed by {lookup_expr}')

                        return lookup_expr

            raise NotImplementedError(f'Unable to find {key_to} for {root_probe}')
        else:
            if key_to == root_part_key:
                lookup_expr = root_probe.key_access(root_probe_key)

                # print(f'column {key_to} is in {root_part}, can be accessed by {lookup_expr}')

                return lookup_expr
            elif key_to == root_probe_key:
                lookup_expr = root_probe.key_access(root_probe_key)

                # print(f'column {key_to} is in {root_probe}, can be accessed by {lookup_expr}')

                return lookup_expr
            elif key_to in root_probe.columns:
                lookup_expr = root_probe.key_access(key_to)

                # print(f'column {key_to} is in {root_probe}, can be accessed by {lookup_expr}')

                return lookup_expr
            elif key_to in root_part.columns:
                lookup_expr = RecAccessExpr(recExpr=DicLookupExpr(dicExpr=root_part.var_part,
                                                                  keyExpr=root_probe.key_access(root_probe_key)),
                                            fieldName=key_to)

                # print(f'column {key_to} is in {root_part}, can be accessed by {lookup_expr}')

                return lookup_expr
            else:
                for part in all_parts:
                    if key_to in part.columns:
                        lookup_expr = Retriever.find_bypass_lookup(all_parts, key_to, root_merge)

                        # print(f'column {key_to} is in {root_part}, can be accessed by {lookup_expr}')

                        return lookup_expr

            raise NotImplementedError(f'Unable to find {key_to} for {root_probe}')

    @staticmethod
    def find_bypass_lookup(all_parts, target, root_merge):
        # print(f'looking for {target} in {all_parts}')

        root_part = root_merge.left
        root_probe = root_merge.right
        root_probe_key = root_merge.right_on

        for this_part in all_parts:
            if target in this_part.columns:
                this_merge = this_part.retriever.find_merge('as_part')
                this_probe_key = this_merge.right_on

                if this_probe_key == root_probe_key:
                    lookup_expr = RecAccessExpr(
                        recExpr=DicLookupExpr(
                            dicExpr=root_part.var_part,
                            keyExpr=root_probe.key_access(root_probe_key)),
                        fieldName=target)
                    return lookup_expr
                elif this_probe_key in root_probe.columns:
                    lookup_expr = RecAccessExpr(
                        recExpr=DicLookupExpr(
                            dicExpr=this_part.var_part,
                            keyExpr=root_probe.key_access(this_probe_key)),
                        fieldName=target)
                    # print(f'found {target} in {root_probe}')
                    return lookup_expr
                else:
                    # print(f'failed to locate {this_probe_key}, go to next session')
                    lookup_expr = RecAccessExpr(
                        recExpr=DicLookupExpr(
                            dicExpr=this_part.var_part,
                            keyExpr=Retriever.find_bypass_lookup(all_parts,
                                                                 this_probe_key,
                                                                 root_merge)),
                        fieldName=target)
                    return lookup_expr
        else:
            raise IndexError(f'Failed to find column {target} in {all_parts}')

    '''
    GroupbyAgg
    '''

    def findall_groupby_aggr(self, body_only=True, drop_last=True):
        expr_list = []
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, GroupbyAggrExpr):
                if body_only:
                    expr_list.append(op_body)
                else:
                    expr_list.append(op_expr)

        if drop_last:
            return expr_list[:-1]
        else:
            return expr_list

    def find_groupby_aggr(self, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """
        for op_expr in reversed(self.history):
            op_body = op_expr.op

            if isinstance(op_body, GroupbyAggrExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr
        else:
            return None

    def find_groupby_aggr_before(self, op_type, body_only=True):
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

            if isinstance(op_body, op_type):
                return None
        else:
            return None

    def find_groupby_aggr_after(self, op_type, body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """
        target_located = False
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, op_type):
                target_located = True

            if isinstance(op_body, GroupbyAggrExpr):
                if target_located:
                    if body_only:
                        return op_body
                    else:
                        return op_expr
        else:
            return None

    '''
    Aggregation
    '''

    def find_aggr(self, body_only=True):
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

    def findall_isin(self, mode='as_probe', body_only=True):
        """

        :return:
        """

        all_isin = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, IsInExpr):
                if mode == 'as_probe':
                    if op_body.probe_on.name == self.target.name:
                        if body_only:
                            all_isin.append(op_body)
                        else:
                            all_isin.append(op_expr)
                if mode == 'as_part':
                    if op_body.part_on.name == self.target.name:
                        if body_only:
                            all_isin.append(op_body)
                        else:
                            all_isin.append(op_expr)
        else:
            return all_isin

    def find_isin(self, mode='as_probe', body_only=True):
        """

        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, IsInExpr):
                if mode == 'as_probe':
                    if op_body.probe_on.name == self.target.name:
                        if body_only:
                            return op_body
                        else:
                            return op_expr
                if mode == 'as_part':
                    if op_body.part_on.name == self.target.name:
                        if body_only:
                            return op_body
                        else:
                            return op_expr
        else:
            return None

    def find_isin_before(self, op_type, mode='as_probe',body_only=True):
        """
        It returns a list that contains all groupby aggregation operations.
        :return:
        """

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, IsInExpr):
                if mode == 'as_probe':
                    if op_body.probe_on.name == self.target.name:
                        if body_only:
                            return op_body
                        else:
                            return op_expr
                if mode == 'as_part':
                    if op_body.part_on.name == self.target.name:
                        if body_only:
                            return op_body
                        else:
                            return op_expr

            if isinstance(op_body, op_type):
                return None
        else:
            return None

    '''
    CalcExpr
    '''

    def find_calc(self, body_only=True):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CalcExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr
        else:
            return None

    '''
    AggrFiltCond
    '''

    def find_aggr_filt(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AggrFiltCond):
                return op_body

        return None

    '''
    
    '''

    @property
    def was_filter(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AggrFiltCond):
                return True

        return False

    @property
    def was_probed(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if op_body.joint.name == self.target.name:
                    return True

            if isinstance(op_body, IsInExpr):
                if op_body.probe_on.name == self.target.name:
                    return True

        return False

    @property
    def was_isin(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, IsInExpr):
                if op_body.probe_on.name == self.target.name:
                    return True

        return False

    @property
    def was_aggregated(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AggrExpr):
                return True

            if isinstance(op_body, GroupbyAggrExpr):
                return True

        return False

    @property
    def was_aggr(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AggrExpr):
                return True

        return False

    @property
    def was_groupby_aggr(self):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, GroupbyAggrExpr):
                return True

        return False

    @property
    def has_multi_gourpby_aggr(self):
        groupby_aggr_list = []
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, GroupbyAggrExpr):
                groupby_aggr_list.append(op_body)

        if len(groupby_aggr_list) > 1:
            return True
        else:
            return False

    def find_col_proj(self, body_only=True) -> ColProjExpr:
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, ColProjExpr):
                if body_only:
                    return op_body
                else:
                    return op_expr
        return None

    def find_col_proj_after(self, op_type, body_only=True):
        target_located = False
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, op_type):
                target_located = True

            if isinstance(op_body, ColProjExpr):
                if target_located:
                    if body_only:
                        return op_body
                    else:
                        return op_expr
        else:
            return None

    @staticmethod
    def equal_expr(expr1: FlexIR, expr2: FlexIR) -> bool:
        if type(expr1) != type(expr2):
            return False

        return expr1.oid == expr2.oid

    def insert_aggr(self, to_which):
        aggr_list = []
        col_ins_list = []

        target_out = {}

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AggrExpr):
                aggr_list.append(op_body)

            if isinstance(op_body, NewColOpExpr):
                col_ins_list.append(op_body.col_var)

        for aggr_expr in aggr_list:
            aggr_name = list(aggr_expr.aggr_op.keys())[0]

            if aggr_name not in col_ins_list:
                new_col_obj = NewColOpExpr(aggr_name, aggr_expr.aggr_op[aggr_name])

                target_out[aggr_name] = aggr_expr.aggr_op[aggr_name]

                op_expr = OpExpr(op_obj=new_col_obj,
                                 op_on=to_which,
                                 op_iter=True,
                                 iter_on=to_which,
                                 ret_type=OpRetType.FLOAT)
                to_which.push(op_expr)

        return target_out

    @staticmethod
    def find_aggr_in_calc(target):
        col_ops = []

        if isinstance(target.unit1, ConstantExpr):
            print(f'Warning: Found a bug to be fixed')
            return col_ops

        if isinstance(target.unit2, ConstantExpr):
            print(f'Warning: Found a bug to be fixed')
            return col_ops

        if isinstance(target.unit1, AggrExpr):
            col_ops.append(target.unit1)

        if isinstance(target.unit2, AggrExpr):
            col_ops.append(target.unit2)

        if isinstance(target.unit1, CalcExpr):
            col_ops += Retriever.find_aggr_in_calc(target.unit1)

        if isinstance(target.unit2, CalcExpr):
            col_ops += Retriever.find_aggr_in_calc(target.unit2)

        return col_ops

    def split_aggr_in_calc(self):
        aggr_expr = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CalcExpr):
                aggr_expr += self.find_aggr_in_calc(op_body)

        return aggr_expr

    def findall_cols_for_calc(self):
        used_cols = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, CalcExpr):
                used_cols = SDQLInspector.find_cols(op_body.sdql_ir)

        return used_cols

    def purify_cond(self, target, accepted):
        if isinstance(target, CondExpr):
            u1_pure = all([i in accepted for i in self.find_cols(target.unit1)])
            u2_pure = all([i in accepted for i in self.find_cols(target.unit2)])

            if u1_pure:
                if u2_pure:
                    raise NotImplementedError
                else:
                    raise NotImplementedError
            else:
                if u2_pure:
                    return target.unit2
                else:
                    raise NotImplementedError


    def residue_cond(self, target, accepted):
        if isinstance(target, CondExpr):
            u1_pure = all([i in accepted for i in self.find_cols(target.unit1)])
            u2_pure = all([i in accepted for i in self.find_cols(target.unit2)])

            if u1_pure:
                if u2_pure:
                    raise NotImplementedError
                else:
                    raise NotImplementedError
            else:
                if u2_pure:
                    return target.unit1
                else:
                    raise NotImplementedError

    def count_except(self, ignore: List, stop=None):
        if not ignore:
            ignore = []

        count = 0

        for op_expr in self.history:
            op_type = op_expr.op_type

            if stop:
                if op_type == stop:
                    break

            if op_type not in ignore:
                count += 1

        return count

    def check_last(self, target):
        if self.equal_expr(self.history[-1].op, target):
            return True

        return False

    @staticmethod
    def find_free_vars(target, as_expr=False) -> dict:
        free_vars = {}

        if isinstance(target, CondExpr):
            u1 = target.unit1
            u2 = target.unit2

            if isinstance(u1, FreeStateVar):
                if as_expr:
                    free_vars[u1.var_name] = u1.var_value
                else:
                    free_vars[u1.var_name] = u1

            if isinstance(u2, FreeStateVar):
                if as_expr:
                    free_vars[u2.var_name] = u2.var_value
                else:
                    free_vars[u2.var_name] = u2

            if isinstance(u1, CondExpr):
                tmp_vars = Retriever.find_free_vars(u1, as_expr)
                for k in tmp_vars.keys():
                    free_vars[k] = tmp_vars[k]

            if isinstance(u2, CondExpr):
                tmp_vars = Retriever.find_free_vars(u2, as_expr)
                for k in tmp_vars.keys():
                    free_vars[k] = tmp_vars[k]
        else:
            print(f'Warning: Unexpected Type {type(target)}')

        return free_vars

    def findall_additional_columns(self):
        additional_columns = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, AddColProj):
                if isinstance(op_body.add_col, list):
                    additional_columns += op_body.add_col
                else:
                    raise NotImplementedError

        return additional_columns

    @staticmethod
    def find_single_aggr_in_calc(target) -> dict:
        single_aggrs = {}

        units = [target.unit1, target.unit2]

        for u in units:
            if isinstance(u, (bool, int, float, str)):
                continue

            elif isinstance(u, AggrExpr):
                if u.is_single_col_op:
                    single_aggrs[u.descriptor] = u

            elif isinstance(u, CalcExpr):
                sub_calc = Retriever.find_single_aggr_in_calc(u)
                for k in sub_calc.keys():
                    single_aggrs[k] = sub_calc[k]

        return single_aggrs

    @staticmethod
    def find_multi_aggr_in_calc(target) -> dict:
        multi_aggrs = {}

        units = [target.unit1, target.unit2]

        for u in units:
            if isinstance(u, (bool, int, float, str)):
                continue

            elif isinstance(u, AggrExpr):
                if u.is_multi_col_op:
                    multi_aggrs[u.descriptor] = u

            elif isinstance(u, CalcExpr):
                sub_calc = Retriever.find_multi_aggr_in_calc(u)
                for k in sub_calc.keys():
                    multi_aggrs[k] = sub_calc[k]

        return multi_aggrs

    @staticmethod
    def find_calc_in_cond(target) -> dict:
        res = {}

        units = [target.unit1, target.unit2]

        for u in units:
            # print(type(u), '\n')
            if isinstance(u, (bool, int, float, str)):
                continue

            elif isinstance(u, CalcExpr):
                res[u.descriptor] = u

            elif isinstance(u, CondExpr):
                sub_cond = Retriever.find_calc_in_cond(u)
                for k in sub_cond.keys():
                    res[k] = sub_cond[k]

        return res

    def find_possible_index_columns(self) -> list:
        possible_index = []

        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, GroupbyAggrExpr):
                possible_index += op_body.groupby_cols

        # print(f'possible index {possible_index}')

        return possible_index

    def check_as_merge_key(self, target):
        for op_expr in self.history:
            op_body = op_expr.op

            if isinstance(op_body, MergeExpr):
                if isinstance(op_body.left_on, list):
                    if target in op_body.left_on:
                        return True
                if isinstance(op_body.right_on, list):
                    if target in op_body.right_on:
                        return True

                if isinstance(op_body.left_on, str):
                    if target == op_body.left_on:
                        return True
                if isinstance(op_body.right_on, str):
                    if target == op_body.right_on:
                        return True