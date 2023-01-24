from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.SDQLIR import SDQLIR
from pysdql.core.dtypes.VarExpr import VarExpr

from pysdql.core.dtypes.sdql_ir import (
    AddExpr,
    MulExpr,
    SubExpr,
    DivExpr,
)

from pysdql.core.dtypes.EnumUtil import (
    MathSymbol,
)

from pysdql.core.dtypes.Utils import (
    input_fmt,
)


class ColExpr(SDQLIR):
    def __init__(self, unit1, operator, unit2):
        self.unit1 = unit1
        self.operator = operator
        self.unit2 = unit2

        # self.__value = value
        # self.relation = relation
        # self.inherit_from = inherit_from
        #
        # self.isvar = self.init_var()

    # @property
    # def col(self):
    #     if isinstance(self.__value, Expr):
    #         return self.__value
    #     else:
    #         raise ValueError()

    # def init_var(self):
    #     from pysdql.core.dtypes.ColEl import ColEl
    #     if type(self.unit1) == ColEl or type(self.unit1) == ColExpr:
    #         if self.unit1.isvar:
    #             if type(self.unit2) == ColEl or type(self.unit1) == ColExpr:
    #                 if self.unit2.isvar:
    #                     return True
    #             if type(self.unit2) in (int, float):
    #                 return True
    #     elif type(self.unit1) in (int, float):
    #         if type(self.unit2) == ColEl or type(self.unit2) == ColExpr:
    #             if self.unit2.isvar:
    #                 return True
    #     return False
    #
    # def new_expr(self, new_str) -> str:
    #     from pysdql.core.dtypes.ColEl import ColEl
    #     if type(self.unit1) == ColExpr or type(self.unit1) == ColEl:
    #         u1_str = self.unit1.new_expr(new_str)
    #     else:
    #         u1_str = f'{self.unit1}'
    #
    #     if type(self.unit2) == ColExpr or type(self.unit2) == ColEl:
    #         u2_str = self.unit2.new_expr(new_str)
    #     else:
    #         u2_str = f'{self.unit2}'
    #
    #     return f'({u1_str} {self.op} {u2_str})'
    #
    # def sum(self):
    #     # tmp_name = f'{self.inherit_from.name}_sum'
    #     sum_expr = SumExpr(sum_on=self.inherit_from,
    #                        sum_func=self.expr,
    #                        sum_if=None,
    #                        sum_else=0.0,
    #                        sum_update=True)
    #     op_expr = OpExpr(op_obj=sum_expr,
    #                      op_on=self.inherit_from,
    #                      op_iter=True,
    #                      iter_on=self.inherit_from,
    #                      ret_type=float)
    #     self.inherit_from.push(op_expr)
    #     return self.inherit_from
    #
    #     # tmp_name = f'agg_val'
    #     # if self.inherit_from:
    #     #     result = VarExpr(tmp_name, IterStmt(self.inherit_from.iter_expr, self.inherit_from.iter_expr.val))
    #     #     self.inherit_from.history_name.append(tmp_name)
    #     #     self.inherit_from.operations.append(OpExpr('colexpr_aggr_sum', result))
    #     # return VarExpr(tmp_name, inherit_from=self.inherit_from)
    #
    # def inherit(self, other):
    #     if self.inherit_from:
    #         pass
    #     else:
    #         self.inherit_from = other.inherit_from
    #
    # @property
    # def expr(self):
    #     return f'({self.unit1} {self.op} {self.unit2})'
    #
    # def __repr__(self):
    #     return self.expr
    #
    # def __add__(self, other):
    #     return ColExpr(unit1=self, operator='+', unit2=other, inherit_from=self.inherit_from)
    #
    # def __radd__(self, other):
    #     return ColExpr(unit1=other, operator='+', unit2=self, inherit_from=self.inherit_from)
    #
    # def __sub__(self, other):
    #     return ColExpr(unit1=self, operator='-', unit2=other, inherit_from=self.inherit_from)
    #
    # def __rsub__(self, other):
    #     return ColExpr(unit1=other, operator='-', unit2=self, inherit_from=self.inherit_from)
    #
    # def __mul__(self, other):
    #     return ColExpr(unit1=self, operator='*', unit2=other, inherit_from=self.inherit_from)
    #
    # def __rmul__(self, other):
    #     return ColExpr(unit1=other, operator='*', unit2=self, inherit_from=self.inherit_from)
    #
    # def __truediv__(self, other):
    #     return ColExpr(unit1=self, operator='/', unit2=other, inherit_from=self.inherit_from)
    #
    # def __rtruediv__(self, other):
    #     return ColExpr(unit1=other, operator='/', unit2=self, inherit_from=self.inherit_from)

    # '''
    # Comparison Operations
    # '''
    #
    # def __eq__(self, other) -> CondExpr:
    #     return CondExpr(unit1=self.col, operator=CompareSymbol.EQ, unit2=input_fmt(other))
    #
    # def __ne__(self, other) -> CondExpr:
    #     return CondExpr(unit1=self.col, operator=CompareSymbol.NE, unit2=input_fmt(other))
    #
    # def __lt__(self, other) -> CondExpr:
    #     return CondExpr(unit1=self.col, operator=CompareSymbol.LT, unit2=input_fmt(other))
    #
    # def __le__(self, other) -> CondExpr:
    #     return CondExpr(unit1=self.col, operator=CompareSymbol.LTE, unit2=input_fmt(other))
    #
    # def __gt__(self, other) -> CondExpr:
    #     return CondExpr(unit1=self.col, operator=CompareSymbol.GT, unit2=input_fmt(other))
    #
    # def __ge__(self, other) -> CondExpr:
    #     return CondExpr(unit1=self.col, operator=CompareSymbol.GTE, unit2=input_fmt(other))

    '''
    Arithmetic Operations
    '''

    def __add__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.ADD,
                       unit2=other)
        # return ColExpr(value=AddExpr(self.col, input_fmt(other)), relation=self.relation)

    def __mul__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.MUL,
                       unit2=other)
        # return ColExpr(value=MulExpr(self.col, input_fmt(other)), relation=self.relation)

    def __sub__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.SUB,
                       unit2=other)
        # return ColExpr(value=SubExpr(self.col, input_fmt(other)), relation=self.relation)

    def __truediv__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.DIV,
                       unit2=other)
        # return ColExpr(value=DivExpr(self.col, input_fmt(other)), relation=self.relation)

    '''
    Reverse Arithmetic Operations
    '''

    def __radd__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.ADD,
                       unit2=self)
        # return ColExpr(value=AddExpr(input_fmt(other), self.col), relation=self.relation)

    def __rmul__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.MUL,
                       unit2=self)
        # return ColExpr(value=MulExpr(input_fmt(other), self.col), relation=self.relation)

    def __rsub__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.SUB,
                       unit2=self)
        # return ColExpr(value=SubExpr(input_fmt(other), self.col), relation=self.relation)

    def __rtruediv__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.DIV,
                       unit2=self)
        # return ColExpr(value=DivExpr(input_fmt(other), self.col), relation=self.relation)

    '''
    Aggregation Function
    '''

    def sum(self):
        raise NotImplemented
        # aggr_expr = AggrExpr(aggr_type=AggrType.VAL,
        #                      aggr_on=self.relation,
        #                      aggr_op=self.col,
        #                      aggr_else=ConstantExpr(0.0))
        #
        # op_expr = OpExpr(op_obj=aggr_expr,
        #                  op_on=self.relation,
        #                  op_iter=True,
        #                  iter_on=self.relation,
        #                  ret_type=float)
        #
        # self.relation.push(op_expr)
        #
        # return aggr_expr

    def replace(self, rec, on=None):
        new_unit1 = self.unit1
        new_unit2 = self.unit2

        if isinstance(self.unit1, SDQLIR):
            new_unit1 = self.unit1.replace(rec, on)
        if isinstance(self.unit2, SDQLIR):
            new_unit2 = self.unit2.replace(rec, on)

        if self.operator == MathSymbol.ADD:
            return AddExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.MUL:
            return MulExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.SUB:
            return SubExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.DIV:
            return DivExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        raise NotImplemented

    @property
    def sdql_ir(self):
        if self.operator == MathSymbol.ADD:
            return AddExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.MUL:
            return MulExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.SUB:
            return SubExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.DIV:
            return DivExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        raise NotImplemented

    def __repr__(self):
        return repr(self.sdql_ir)
