import re
from datetime import datetime

from enum import (
    Enum,
    unique,
)

from pysdql.core.dtypes.SDQLIR import SDQLIR
from pysdql.core.dtypes.Utils import (
    input_fmt
)
from pysdql.core.dtypes.sdql_ir import (
    Expr,
    RecAccessExpr,
    CompareSymbol,
    CompareExpr,
    MulExpr,
    AddExpr,
)


from pysdql.core.dtypes.EnumUtil import (
    LogicSymbol,
)


class CondExpr(SDQLIR):
    def __init__(self, unit1, operator, unit2, inherit_from=None, isin=False, isjoin=False):
        self.unit1 = unit1
        self.op = operator
        self.unit2 = unit2
        # self.inherit_from = inherit_from
        # self.isin = isin
        # self.isjoin = isjoin

        # self.date_fmt()

    # def date_fmt(self):
    #     if self.is_date(self.unit1):
    #         date1 = self.parse_date(self.unit1)
    #         m = str(date1.month)
    #         d = str(date1.day)
    #         if len(m) == 1:
    #             m = f'0{m}'
    #         if len(d) == 1:
    #             d = f'0{d}'
    #         # self.unit1 = f'date({date1.year}{m}{d})'
    #         self.unit1 = f'{date1.year}{m}{d}'
    #
    #     if self.is_date(self.unit2):
    #         date2 = self.parse_date(self.unit2)
    #         m = str(date2.month)
    #         d = str(date2.day)
    #         if len(m) == 1:
    #             m = f'0{m}'
    #         if len(d) == 1:
    #             d = f'0{d}'
    #         # self.unit2 = f'date({date2.year}{m}{d})'
    #         self.unit2 = f'{date2.year}{m}{d}'
    #
    # @staticmethod
    # def parse_date(data):
    #     return datetime.strptime(data.replace('"', ''), '%Y-%m-%d')
    #
    # @staticmethod
    # def is_date(data):
    #     if type(data) == str:
    #         pattern = re.compile(r'("\d{4}-\d{2}-\d{2})"')
    #         if pattern.findall(data.strip()):
    #             return True
    #     return False

    # def inherit(self, other):
    #     if type(other) == CondExpr:
    #         if other.inherit_from:
    #             if self.inherit_from:
    #                 self.inherit_from.inherit(other.inherit_from)
    #             else:
    #                 self.inherit_from = other.inherit_from
    #         if self.isin or other.isin:
    #             self.isin = True
    #     return self
    #
    # def new_cond(self, new_str):
    #     from pysdql.core.dtypes.ColEl import ColEl
    #     if type(self.unit1) == ColEl or type(self.unit1) == CondExpr:
    #         u1_str = self.unit1.new_expr(new_str)
    #     else:
    #         u1_str = str(self.unit1)
    #     if type(self.unit2) == ColEl or type(self.unit2) == CondExpr:
    #         u2_str = self.unit2.new_expr(new_str)
    #     else:
    #         u2_str = str(self.unit2)
    #
    #     return self.concat(u1_str, u2_str)
    #
    # def new_expr(self, new_str) -> str:
    #     from pysdql.core.dtypes.ColEl import ColEl
    #     if type(self.unit1) == ColEl or type(self.unit1) == CondExpr:
    #         u1_str = self.unit1.new_expr(new_str)
    #     else:
    #         u1_str = str(self.unit1)
    #     if type(self.unit2) == ColEl or type(self.unit2) == CondExpr:
    #         u2_str = self.unit2.new_expr(new_str)
    #     else:
    #         u2_str = str(self.unit2)
    #
    #     if self.op in ['==', '<=', '<']:
    #         return f'({u1_str} {self.op} {u2_str})'
    #     if self.op == '>':
    #         return f'({u2_str} < {u1_str})'
    #     if self.op == '>=':
    #         return f'({u2_str} <= {u1_str})'
    #     if self.op == '!=':
    #         return f'({u1_str} != {u2_str})'
    #     if self.op == '&&':
    #         return f'({u1_str} {self.op} {u2_str})'
    #     if self.op == '||':
    #         return f'({u1_str} {self.op} {u2_str})'
    #     if self.op == '~':
    #         return f'(!({u1_str}))'
    #     return f'({u1_str} {self.op} {u2_str})'
    #
    # def concat(self, u1, u2):
    #     if self.op in ['<', '<=', '==', '!=', '&&', '||', '~']:
    #         return CondExpr(u1, self.op, u2).inherit(self)
    #     if self.op == '>':
    #         return CondExpr(u2, '<', u1).inherit(self)
    #     if self.op == '>=':
    #         return CondExpr(u2, '<=', u1).inherit(self)
    #     return CondExpr(u1, self.op, u2).inherit(self)
    #
    # @property
    # def expr(self):
    #     if self.op in ['==', '<=', '<']:
    #         return f'({self.unit1} {self.op} {self.unit2})'
    #     if self.op == '>':
    #         return f'({self.unit2} < {self.unit1})'
    #     if self.op == '>=':
    #         return f'({self.unit2} <= {self.unit1})'
    #     if self.op == '!=':
    #         return f'({self.unit1} != {self.unit2})'
    #     if self.op == '&&':
    #         # return f'({self.unit1} {self.op} {self.unit2})'
    #         return f'({self.unit1} and {self.unit2})'
    #     if self.op == '||':
    #         # return f'({self.unit1} {self.op} {self.unit2})'
    #         return f'({self.unit1} or {self.unit2})'
    #     if self.op == '~':
    #         return f'(!({self.unit1}))'
    #     return f'({self.unit1} {self.op} {self.unit2})'

    # def __str__(self):
    #     return self.expr
    #
    # def __repr__(self):
    #     return self.expr
    #
    # def __and__(self, other):
    #     return CondExpr(unit1=self,
    #                     operator='&&',
    #                     unit2=other).inherit(other)
    #
    # def __rand__(self, other):
    #     return CondExpr(unit1=other,
    #                     operator='&&',
    #                     unit2=self).inherit(other)
    #
    # def __iand__(self, other):
    #     return (self & other).inherit(other)
    #
    # def __or__(self, other):
    #     return CondExpr(unit1=self,
    #                     operator='||',
    #                     unit2=other).inherit(other)
    #
    # def __ror__(self, other):
    #     return CondExpr(unit1=other,
    #                     operator='||',
    #                     unit2=self).inherit(other)
    #
    # def __ior__(self, other):
    #     return CondExpr(unit1=self,
    #                     operator='||',
    #                     unit2=other, inherit_from=self.inherit_from).inherit(other)
    #
    # def __invert__(self):
    #     return CondExpr(unit1=self,
    #                     operator='~',
    #                     unit2=self).inherit(self)

    '''
    AND, OR, NOT
    '''

    def __and__(self, other):
        return CondExpr(unit1=self.sdql_ir,
                        operator=LogicSymbol.AND,
                        unit2=input_fmt(other))
        # return MulExpr(self.sdql_ir, input_fmt(other))

    def __or__(self, other):
        return CondExpr(unit1=self.sdql_ir,
                        operator=LogicSymbol.OR,
                        unit2=input_fmt(other))
        # return AddExpr(self.sdql_ir, input_fmt(other))

    def __invert__(self):
        # return CondExpr(unit1=self,
        #                 operator=LogicSymbol.NOT,
        #                 unit2=self)
        raise NotImplemented

    '''
    Reverse AND, OR
    '''

    def __rand__(self, other):
        # return CondExpr(unit1=other,
        #                 operator=LogicSymbol.AND,
        #                 unit2=self)
        return MulExpr(input_fmt(other), self.sdql_ir)

    def __ror__(self, other):
        # return CondExpr(unit1=other,
        #                 operator=LogicSymbol.OR,
        #                 unit2=self)
        return AddExpr(input_fmt(other), self.sdql_ir)

    '''
    Recursive AND, OR
    '''

    def __iand__(self, other):
        return self & other

    def __ior__(self, other):
        return self | other

    @property
    def sdql_ir(self):
        if self.op == LogicSymbol.AND:
            return MulExpr(self.unit1, self.unit2)
        if self.op == LogicSymbol.OR:
            return AddExpr(self.unit1, self.unit2)
        if self.op == LogicSymbol.NOT:
            raise NotImplemented()

        if isinstance(self.op, CompareSymbol):
            return CompareExpr(self.op, self.unit1, self.unit2)

    @property
    def op_name_suffix(self):
        return f'_filter'

    def __repr__(self):
        return repr(self.sdql_ir)
