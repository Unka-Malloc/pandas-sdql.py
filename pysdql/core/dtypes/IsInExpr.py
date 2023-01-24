from pysdql.core.dtypes.IgnoreExpr import IgnoreExpr
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

    def get_non_null(self, replace=None):
        if replace:
            body = replace
        else:
            body = DicLookupExpr(self.part_on.get_var_part(),
                                 self.col_probe.sdql_ir)

        if self.isinvert:
            return CompareExpr(CompareSymbol.EQ,
                               body,
                               ConstantExpr(None))
        else:
            return CompareExpr(CompareSymbol.NE,
                               body,
                               ConstantExpr(None))

    def get_isin_part(self):
        part_var = self.part_on.get_var_part()
        part_key = self.part_on.iter_el.key
        part_retriever = self.part_on.get_retriever()

        sum_op = DicConsExpr([(self.col_part, ConstantExpr(True))])

        cond = part_retriever.find_cond_before(IsInExpr)
        if cond:
            sum_op = IfExpr(condExpr=cond,
                            thenBodyExpr=sum_op,
                            elseBodyExpr=ConstantExpr(None))

        sum_expr = SumExpr(varExpr=part_key,
                           dictExpr=part_var,
                           bodyExpr=sum_op,
                           isAssignmentSum=True)

        let_expr = LetExpr(varExpr=part_var,
                           valExpr=sum_expr,
                           bodyExpr=ConstantExpr(True))

        return let_expr

    def get_ref_var(self):
        return self.part_on.get_var_part()

    def get_ref_ir(self, next_op=None):
        if not next_op:
            next_op = ConstantExpr('placeholder_isin_ref')

        opt = self.part_on.get_opt()
        part_var = self.get_ref_var()
        col_proj = opt.col_proj

        sum_op = DicConsExpr([(self.part_on.key_access(self.col_part.field),
                               ConstantExpr(True))])

        if opt.has_cond:
            cond_after_groupby_agg = opt.get_cond_after_groupby_agg()
            if cond_after_groupby_agg:
                return opt.get_groupby_agg_having_stmt(next_op)
            else:
                cond = opt.get_cond_ir()
                sum_op = IfExpr(condExpr=cond,
                                thenBodyExpr=sum_op,
                                elseBodyExpr=EmptyDicConsExpr())
                sum_expr = SumExpr(varExpr=self.part_on.iter_el.el,
                                   dictExpr=self.part_on.var_expr,
                                   bodyExpr=sum_op,
                                   isAssignmentSum=True)
        else:
            cond = opt.get_cond_ir()
            sum_op = IfExpr(condExpr=cond,
                            thenBodyExpr=sum_op,
                            elseBodyExpr=EmptyDicConsExpr())

            sum_expr = SumExpr(varExpr=self.part_on.iter_el.el,
                               dictExpr=self.part_on.var_expr,
                               bodyExpr=sum_op,
                               isAssignmentSum=True)

        let_expr = LetExpr(varExpr=part_var,
                           valExpr=sum_expr,
                           bodyExpr=next_op)
        return let_expr

    def __invert__(self):
        self.isinvert = True
        return self

    def __repr__(self):
        return f'{self.probe_on.name}.{self.col_probe} in {self.part_on.name}.{self.col_part}'

    @property
    def op_name_suffix(self):
        return ''
