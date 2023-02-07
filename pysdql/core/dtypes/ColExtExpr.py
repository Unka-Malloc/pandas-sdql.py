from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.ColOpExpr import ColOpExpr
from pysdql.core.dtypes.EnumUtil import MathSymbol, LogicSymbol

from pysdql.core.dtypes.sdql_ir import *


class ColExtExpr(FlexIR):
    def __init__(self, col, ext_func, args=None, isinvert=False):
        """

        :param ColEl col:
        :param ext_func:
        :param args:
        :param isinvert:
        """
        self.col = col
        self.func = ext_func
        self.args = args

        self.vars = {}

        self.isinvert = isinvert

    def replace(self, rec, inplace=False, mapper=None):
        return ColExtExpr(self.col.replace(rec, inplace, mapper), self.func, self.args, self.isinvert)

    def gen_cond_expr(self, operator, unit2):
        """
        :param operator: CompareSymbol
        :param unit2: ColEl | (float, int, str) | date@str
        :return:
        """
        if operator == CompareSymbol.EQ:
            if type(unit2) == str:
                self.col.add_const(unit2)
                return CondExpr(unit1=self, operator=operator, unit2=self.col.get_const_var(unit2))
            return CondExpr(unit1=self,
                            operator=operator,
                            unit2=unit2)

        return CondExpr(unit1=self,
                        operator=operator,
                        unit2=unit2)

    def __eq__(self, other) -> CondExpr:
        """
        Equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.EQ,
                                  unit2=other)
        # if type(other) == str:
        #     self.add_const(other)
        #     return CondExpr(unit1=self.col, operator=CompareSymbol.EQ, unit2=self.get_const_var(other))
        # return CondExpr(unit1=self.col, operator=CompareSymbol.EQ, unit2=input_fmt(other))

    def __ne__(self, other) -> CondExpr:
        """
        Not equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.NE,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.NE, unit2=input_fmt(other))

    def __lt__(self, other) -> CondExpr:
        """
        Less than
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.LT,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.LT, unit2=input_fmt(other))

    def __le__(self, other) -> CondExpr:
        """
        Less than or Equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.LTE,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.LTE, unit2=input_fmt(other))

    def __gt__(self, other) -> CondExpr:
        """
        Greater than
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.GT,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.GT, unit2=input_fmt(other))

    def __ge__(self, other) -> CondExpr:
        """
        Greater than or Equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.GTE,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.GTE, unit2=input_fmt(other))

    def __and__(self, other):
        return CondExpr(unit1=self,
                        operator=LogicSymbol.AND,
                        unit2=other)

    def __or__(self, other):
        return CondExpr(unit1=self,
                        operator=LogicSymbol.OR,
                        unit2=other)

    def __invert__(self):
        return CondExpr(unit1=self,
                        operator=LogicSymbol.NOT,
                        unit2=self)

    def __add__(self, other):
        return ColOpExpr(unit1=self,
                         operator=MathSymbol.ADD,
                         unit2=other)

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return True

    @property
    def oid(self):
        return hash((
            self.col.oid,
            self.func,
            self.args
        ))

    @property
    def sdql_ir(self):
        if isinstance(self.col, FlexIR):
            col_expr = self.col.sdql_ir
        elif isinstance(self.col, Expr):
            col_expr = self.col
        else:
            raise TypeError(f'Illegal Column IR {type(self.col)} {self.col}')

        if self.func == ExtFuncSymbol.StartsWith:
            return ExtFuncExpr(self.func,
                               col_expr,
                               self.args,
                               ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.EndsWith:
            return ExtFuncExpr(self.func,
                               col_expr,
                               self.args,
                               ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.StringContains:
            return CompareExpr(CompareSymbol.NE,
                               ExtFuncExpr(ExtFuncSymbol.FirstIndex,
                                           col_expr,
                                           self.args,
                                           ConstantExpr("Nothing!")),
                               MulExpr(ConstantExpr(-1), ConstantExpr(1)))

        if self.func == ExtFuncSymbol.FirstIndex:
            return ExtFuncExpr(self.func,
                                col_expr,
                                self.args,
                                ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.ExtractYear:
            return ExtFuncExpr(self.func,
                                col_expr,
                                ConstantExpr("Nothing!"),
                                ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.SubStr:
            return ExtFuncExpr(self.func,
                                col_expr,
                                ConstantExpr(int(self.args[0])),
                                ConstantExpr(int(self.args[1]) - 1))

        raise NotImplementedError(f'''
        {self.col},
        {self.func}
        ''')

    def __repr__(self):
        return str(self.sdql_ir)
