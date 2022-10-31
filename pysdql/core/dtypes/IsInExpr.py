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
            cond = opt.get_cond_ir()
            sum_op = IfExpr(condExpr=cond,
                            thenBodyExpr=sum_op,
                            elseBodyExpr=EmptyDicConsExpr())

        sum_expr = SumExpr(varExpr=self.part_on.iter_el.key,
                           dictExpr=self.part_on.var_expr,
                           bodyExpr=sum_op,
                           isAssignmentSum=True)

        let_expr = LetExpr(varExpr=part_var,
                           valExpr=sum_expr,
                           bodyExpr=next_op)
        return let_expr

    @property
    def op_name_suffix(self):
        return ''

    # @property
    # def expr(self):
    #     return f'{self.col_probe} == {self.col_part}'
    #
    # def __repr__(self):
    #     return str(self.cond)
    #
    # @property
    # def cond(self):
    #     if self.isinvert:
    #         return ~CondExpr(self.col_probe, '==', self.col_part, inherit_from=self.col_part.relation, isin=True)
    #     else:
    #         return CondExpr(self.col_probe, '==', self.col_part, inherit_from=self.col_part.relation, isin=True)
    #
    # def __and__(self, other):
    #     return CondExpr(unit1=self.cond,
    #                     operator='&&',
    #                     unit2=other).inherit(self.cond).inherit(other)
    #
    # def __rand__(self, other):
    #     return CondExpr(unit1=other,
    #                     operator='&&',
    #                     unit2=self.cond).inherit(self.cond).inherit(other)
    #
    # def __or__(self, other):
    #     return CondExpr(unit1=self.cond,
    #                     operator='||',
    #                     unit2=other).inherit(self.cond).inherit(other)
    #
    # def __ror__(self, other):
    #     return CondExpr(unit1=other,
    #                     operator='||',
    #                     unit2=self.cond).inherit(self.cond).inherit(other)
    #
    # def __invert__(self):
    #     self.isinvert = True
    #     return self
