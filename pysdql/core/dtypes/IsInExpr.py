from pysdql.core.dtypes.GroupbyAggrExpr import GroupbyAggrExpr
from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.dtypes.sdql_ir import *


class IsInExpr(IgnoreExpr):
    def __init__(self, col_probe, col_part, invert=False):
        self.col_probe = col_probe
        self.probe_on = col_probe.relation
        self.col_part = col_part
        self.part_on = col_part.relation
        self.isinvert = invert

    @property
    def ignore(self):
        return True

    def get_probe_field(self):
        return self.col_probe.field

    def get_as_cond(self, replace=None):
        if replace:
            body = replace
        else:
            body = DicLookupExpr(self.part_on.get_var_part(),
                                 self.col_probe.sdql_ir)

        body_other = ConstantExpr(None)

        if self.isinvert:
            return CompareExpr(CompareSymbol.EQ,
                               body,
                               body_other)
        else:
            return CompareExpr(CompareSymbol.NE,
                               body,
                               body_other)

    def get_as_part(self, next_op=None):
        part_var = self.part_on.get_var_part()
        part_retriever = self.part_on.get_retriever()

        groupby_aggr_info = part_retriever.find_groupby_aggr()

        merge_info = part_retriever.find_merge('as_joint')

        if merge_info:
            if next_op:
                return merge_info.joint.joint_frame.get_joint_expr(next_op)
            else:
                return merge_info.joint.joint_frame.get_joint_expr(ConstantExpr(True))

        if groupby_aggr_info:
            # Q18

            groupby_cols = groupby_aggr_info.groupby_cols
            aggr_dict = groupby_aggr_info.aggr_dict

            vname_aggr = f'{self.part_on.name}_aggr'
            var_aggr = VarExpr(vname_aggr)
            vname_x_aggr = f'x_{vname_aggr}'
            var_x_aggr = VarExpr(vname_x_aggr)

            cond_after_aggr = part_retriever.find_cond_after(GroupbyAggrExpr)

            if self.col_part.field not in groupby_cols:
                raise IndexError(f'Cannot find column {self.col_part.field}!')

            cond_mapper = {}

            # aggr = {? : scalar}
            if len(aggr_dict.keys()) == 1:
                cond_mapper[tuple(list(aggr_dict.keys()))] = PairAccessExpr(var_x_aggr, 1)

            # aggr = {? : record}
            else:
                for k in aggr_dict.keys():
                    cond_mapper[tuple([k])] = RecAccessExpr(PairAccessExpr(var_x_aggr, 1), k)

            # aggr = {scalar: record}
            if len(groupby_cols) == 1:
                cond_mapper[tuple(groupby_cols)] = PairAccessExpr(var_x_aggr, 0)

                sum_op_isin = DicConsExpr([(PairAccessExpr(var_x_aggr, 0), ConstantExpr(True))])
            else:
                # aggr = {record : record}
                for c in groupby_cols:
                    cond_mapper[tuple([c])] = RecAccessExpr(PairAccessExpr(var_x_aggr, 0), c)

                sum_op_isin = DicConsExpr([(RecAccessExpr(PairAccessExpr(var_x_aggr, 0), self.col_part.field),
                                            ConstantExpr(True))])

            cond_after_aggr = cond_after_aggr.replace(rec=None,
                                                      inplace=True,
                                                      mapper=cond_mapper)

            if cond_after_aggr:
                sum_op_isin = IfExpr(condExpr=cond_after_aggr,
                                     thenBodyExpr=sum_op_isin,
                                     elseBodyExpr=ConstantExpr(None))

            sum_expr_isin = SumExpr(varExpr=var_x_aggr,
                                    dictExpr=var_aggr,
                                    bodyExpr=sum_op_isin,
                                    isAssignmentSum=True)

            if next_op:
                let_expr_isin = LetExpr(varExpr=part_var,
                                        valExpr=sum_expr_isin,
                                        bodyExpr=next_op)
            else:
                let_expr_isin = LetExpr(varExpr=part_var,
                                        valExpr=sum_expr_isin,
                                        bodyExpr=ConstantExpr(True))

            return self.part_on.get_groupby_aggr(let_expr_isin)
        else:
            cond = part_retriever.find_cond_before(IsInExpr)

            sum_op = DicConsExpr([(self.col_part.replace(self.part_on.iter_el.key),
                                   ConstantExpr(True))])

            if cond:
                sum_op = IfExpr(condExpr=cond.sdql_ir,
                                thenBodyExpr=sum_op,
                                elseBodyExpr=ConstantExpr(None))

            sum_expr = SumExpr(varExpr=self.part_on.iter_el.el,
                               dictExpr=self.part_on.var_expr,
                               bodyExpr=sum_op,
                               isAssignmentSum=True)

            if next_op:
                let_expr = LetExpr(varExpr=part_var,
                                   valExpr=sum_expr,
                                   bodyExpr=next_op)
            else:
                let_expr = LetExpr(varExpr=part_var,
                                   valExpr=sum_expr,
                                   bodyExpr=ConstantExpr(True))

        return let_expr

    def get_ref_var(self):
        return self.part_on.get_var_part()

    def __invert__(self):
        self.isinvert = True
        return self

    def __repr__(self):
        return f'{self.probe_on.name}.{self.col_probe.field} is in {self.part_on.name}.{self.col_part.field}'

    @property
    def op_name_suffix(self):
        return ''
