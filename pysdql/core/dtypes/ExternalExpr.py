from pysdql.core.dtypes.SDQLIR import SDQLIR
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.EnumUtil import MathSymbol, LogicSymbol

from pysdql.core.dtypes.sdql_ir import *


class ExternalExpr(SDQLIR):
    def __init__(self, col, ext_func, args=None, isinvert=False):
        self.col = col
        self.func = ext_func
        self.args = args

        self.vars = {}

        self.isinvert = isinvert

    def replace(self, rec, on=None):
        return ExternalExpr(self.col.replace(rec, on), self.func, self.args, self.isinvert)

    def gen_cond_expr(self, operator, unit2):
        """
        :param operator: ColEl
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

    def __add__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.ADD,
                       unit2=other)

    @property
    def sdql_ir(self):
        if not isinstance(self.col, SDQLIR):
            raise TypeError(f'Illegal Column IR')

        if self.func == ExtFuncSymbol.StartsWith:
            return ExtFuncExpr(self.func,
                               self.col.sdql_ir,
                               self.args,
                               ConstantExpr("Nothing!"))
        if self.func == ExtFuncSymbol.StringContains:
            return CompareExpr(CompareSymbol.NE,
                               ExtFuncExpr(ExtFuncSymbol.FirstIndex,
                                           self.col.sdql_ir,
                                           self.args,
                                           ConstantExpr("Nothing!")),
                               MulExpr(ConstantExpr(-1), ConstantExpr(1)))
        if self.func == ExtFuncSymbol.FirstIndex:
            return ExtFuncExpr(self.func,
                                self.col.sdql_ir,
                                self.args,
                                ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.ExtractYear:
            return ExtFuncExpr(self.func,
                                self.col.sdql_ir,
                                ConstantExpr("Nothing!"),
                                ConstantExpr("Nothing!"))

        raise NotImplementedError(f'''
        {self.col},
        {self.func}
        ''')

    def __repr__(self):
        return str(self.sdql_ir)

    # @property
    # def vars_str(self):
    #     tmp_list = []
    #     if self.vars:
    #         tmp_dict = self.vars
    #         for k in tmp_dict.keys():
    #             tmp_list.append(f'let {k} = {tmp_dict[k]}')
    #     return ' '.join(tmp_list)
    #
    # def new_expr(self, new_str) -> str:
    #     if self.func == 'Year':
    #         return f'ext(`{self.func}`, {self.col.new_expr(new_str)})'
    #     if self.func == 'StrStartsWith':
    #         if self.isinvert:
    #             return f'!(ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}"))'
    #         return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
    #     if self.func == 'StrEndsWith':
    #         if self.isinvert:
    #             return f'!(ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}"))'
    #         return f'ext(`{self.func}`, {self.col.new_expr(new_str)}, "{self.args}")'
    #     if self.func == 'StrContains':
    #         tmp_str = ''
    #         for i in range(len(self.args)):
    #             tmp_str += f'"{self.args[i]}"'
    #             if i < len(self.args) - 1:
    #                 tmp_str += ', '
    #         if self.isinvert:
    #             return f'!(ext(`StrContainsN`, {self.col.new_expr(new_str)}, {tmp_str}))'
    #         return f'ext(`StrContainsN`, {self.col.new_expr(new_str)}, {tmp_str})'
    #     if self.func == 'not_StrContains':
    #         pass
    #     if self.func == 'SubString':
    #         return f'ext(`SubString`, {self.col.new_expr(new_str)}, {self.args[0]}, {self.args[1]})'
    #     if self.func == 'StrContains_in_order':
    #         for i in range(len(self.args)):
    #             if i == 0:
    #                 self.vars[f'idx{i}'] = f'ext(`StrIndexOf`, {self.col.new_expr(new_str)}, "{self.args[i]}", {i})'
    #             else:
    #                 self.vars[f'idx{i}'] = f'ext(`StrIndexOf`, {self.col.new_expr(new_str)}, "{self.args[i]}", idx{i - 1})'
    #         tmp_str = ''
    #         for k in range(len(self.vars.keys())):
    #             tmp_str += f'({list(self.vars.keys())[k]} != -1)'
    #             if k != len(self.vars.keys()) - 1:
    #                 tmp_str += f' && '
    #         if self.isinvert:
    #             return f'!({tmp_str})'
    #         else:
    #             return tmp_str
    #     if self.func == 'StrIndexOf':
    #         return f'ext(`StrIndexOf`, {self.col.new_expr(new_str)}, "{self.args[0]}", {self.args[1]})'
    #
    # @property
    # def expr(self):
    #     if self.func == 'Year':
    #         return f'ext(`Year`, {self.col})'
    #     if self.func == 'StrStartsWith':
    #         return f'ext(`StrStartsWith`, {self.col.relation.iter_expr.key}.{self.col.field}, "{self.args}")'
    #     if self.func == 'StrEndsWith':
    #         return f'ext(`StrEndsWith`, {self.col}, "{self.args}")'
    #     if self.func == 'StrContains':
    #         tmp_str = ''
    #         for i in range(len(self.args)):
    #             tmp_str += f'"{self.args[i]}"'
    #             if i < len(self.args) - 1:
    #                 tmp_str += ', '
    #         return f'ext(`StrContainsN`, {self.col}, {tmp_str})'
    #     if self.func == 'not_StrContains':
    #         tmp_str = ''
    #         for i in range(len(self.args)):
    #             tmp_str += f'"{self.args[i]}"'
    #             if i < len(self.args) - 1:
    #                 tmp_str += ', '
    #         return f'!(ext(`StrContainsN`, {self.col}, {tmp_str}))'
    #     if self.func == 'SubString':
    #         return f'ext(`SubString`, {self.col}, {self.args[0]}, {self.args[1]})'
    #
    # def __repr__(self):
    #     return self.expr
    #
    # def __invert__(self):
    #     if self.func in ('StrStartsWith', 'StrEndsWith', 'StrContains'):
    #         return f'!({self.expr})'
    #     else:
    #         self.isinvert = True
    #         return self
    #
    # def isin(self, vals):
    #     return self.col.isin(vals, ext=self.expr)
