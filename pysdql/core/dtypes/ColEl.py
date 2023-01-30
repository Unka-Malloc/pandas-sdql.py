from pysdql.core.dtypes.ExtDateTime import ExtDatetime
from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.EnumUtil import AggrType, OpRetType
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.ExistExpr import ExistExpr
from pysdql.core.dtypes.SDQLIR import SDQLIR
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.ExternalExpr import ExternalExpr

from pysdql.core.dtypes.sdql_ir import (
    CompareSymbol,
    ExtFuncSymbol,
    RecAccessExpr,
    ConstantExpr,

    AddExpr,
    MulExpr,
    SubExpr,
    DivExpr,
)

from pysdql.core.dtypes.Utils import (
    is_date,
    input_fmt,
)

from pysdql.core.dtypes.EnumUtil import (
    MathSymbol
)


class ColEl(SDQLIR):
    def __init__(self, relation, field: str, promoted=None):
        """
        ColUnit 在被创建的时候总是作为Relation的元素出现，因此必定存在IterExpr
        :param relation: DataFrame
        :param field:
        """
        self.__relation = relation
        self.__field = field
        self.promoted = promoted
        self.follow_promotion = None
        self.data_type = ''

        self.isvar = False
        self.var_name = ''

    def add_const(self, const):
        self.relation.add_const(const)

    def get_const_var(self, const):
        return self.relation.get_const_var(const)

    @property
    def relation(self):
        return self.__relation

    @property
    def R(self):
        return self.__relation

    @property
    def field(self):
        return self.__field

    @property
    def col(self):
        return self.relation.key_access(self.field)

    @property
    def year(self):
        return ExternalExpr(col=self, ext_func=ExtFuncSymbol.ExtractYear)

    @property
    def month(self):
        raise NotImplementedError

    @property
    def day(self):
        raise NotImplementedError

    def new_expr(self, new_str) -> str:
        if self.isvar:
            if self.promoted:
                f'promote[real]({self.var_name})'
            return f'{self.var_name}'
        return f'{new_str}.{self.field}'

    @property
    def from_1DT(self):
        if self.relation.structure == '1DT':
            return True
        else:
            return False
        # from pysdql.core.dtypes.relation import relation
        # if type(self.relation) == relation:
        #     return True
        # else:
        #     return False

    @property
    def from_LRT(self):
        if self.relation.structure == 'LRT':
            return True
        else:
            return False
        # from pysdql.core.dtypes.JoinExpr import JoinExpr
        # if type(self.dataframe) == JoinExpr:
        #     return True
        # else:
        #     return False

    @property
    def from_GRP(self):
        if self.relation.structure == 'GRP':
            return True
        else:
            return False

    # @property
    # def key(self):
    #     if self.from_1DT:
    #         return self.relation.el.k
    #     if self.from_LRT:
    #         if self.field in self.relation.left.columns:
    #             return f'{self.relation.el.k}.left'
    #         if self.field in self.relation.right.columns:
    #             return f'{self.relation.el.k}.right'
    #     if self.from_GRP:
    #         if self.field in self.relation.groupby.columns:
    #             return self.relation.el.k
    #
    # @property
    # def val(self):
    #     if self.from_1DT:
    #         return self.relation.el.v
    #     if self.from_LRT:
    #         return self.relation.el.v
    #     if self.from_GRP:
    #         return 1

    @property
    def expr(self) -> str:
        return f'{self.relation.el.k}.{self.field}'
        # if self.isvar:
        #     if self.promoted:
        #         f'promote[real]({self.var_name})'
        #     return self.var_name
        # if self.from_LRtuple:
        #     if self.name in self.dataframe.left.cols:
        #         return f'{self.dataframe.el.k}.left.{self.name}'
        #     if self.name in self.dataframe.right.cols:
        #         return f'{self.dataframe.el.k}.right.{self.name}'
        #     else:
        #         raise ValueError()
        # if self.follow_promotion:
        #     return f'{self.follow_promotion}({self.dataframe.el.k}.{self.name})'
        # return f'{self.dataframe.el.k}.{self.name}'

    @property
    def sdql_expr(self) -> str:
        return f'{self.relation.el.k}.{self.field}'

    def __str__(self):
        return self.sdql_expr

    def __repr__(self):
        return self.expr

    def __hash__(self):
        return hash((self.R.el.k, self.R.el.v, self.R.name, self.field))

    '''
    Comparison Operations
    '''

    def gen_cond_expr(self, operator, unit2):
        """
        :param operator: ColEl
        :param unit2: ColEl | (float, int, str) | date@str
        :return:
        """
        if operator == CompareSymbol.EQ or operator == CompareSymbol.NE:
            if type(unit2) == str:
                self.add_const(unit2)
                return CondExpr(unit1=self, operator=operator, unit2=self.get_const_var(unit2))
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

    '''
    Arithmetic Operations
    '''

    def __add__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.ADD,
                       unit2=other)
        # return ColExpr(value=AddExpr(self.col, input_fmt(other)), relation=self.R)

    def __mul__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.MUL,
                       unit2=other)
        # return ColExpr(value=MulExpr(self.col, input_fmt(other)), relation=self.R)

    def __sub__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.SUB,
                       unit2=other)
        # return ColExpr(value=SubExpr(self.col, input_fmt(other)), relation=self.R)

    def __truediv__(self, other):
        return ColExpr(unit1=self,
                       operator=MathSymbol.DIV,
                       unit2=other)
        # return ColExpr(value=DivExpr(self.col, input_fmt(other)), relation=self.R)

    '''
    Reverse Arithmetic Operations
    '''

    def __radd__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.ADD,
                       unit2=self)
        # return ColExpr(value=AddExpr(input_fmt(other), self.col), relation=self.R)

    def __rmul__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.MUL,
                       unit2=self)
        # return ColExpr(value=MulExpr(input_fmt(other), self.col), relation=self.R)

    def __rsub__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.SUB,
                       unit2=self)
        # return ColExpr(value=SubExpr(input_fmt(other), self.col), relation=self.R)

    def __rtruediv__(self, other):
        return ColExpr(unit1=other,
                       operator=MathSymbol.DIV,
                       unit2=self)
        # return ColExpr(value=DivExpr(input_fmt(other), self.col), relation=self.R)

    def isin(self, vals):
        if type(vals) == list or type(vals) == tuple:
            if len(vals) == 0:
                raise ValueError()
            if len(vals) == 1:
                return vals[0]

            tmp_list = []
            for i in vals:
                tmp_list.append(self.gen_cond_expr(operator=CompareSymbol.EQ,
                                                   unit2=i))

            a = tmp_list.pop()
            b = tmp_list.pop()

            tmp_cond = a | b

            if tmp_list:
                for i in tmp_list:
                    tmp_cond |= i

            return tmp_cond

        if type(vals) == ColEl:
            isin_expr = IsInExpr(col_probe=self, col_part=vals)

            for k in vals.relation.context_constant:
                self.add_const(k)

            self.relation.push(OpExpr(op_obj=isin_expr,
                                      op_on=self.R,
                                      op_iter=True,
                                      iter_on=None,
                                      ret_type=None))
            return isin_expr

    # def isin(self, vals, ext=None):
    #     # print(f'{self.expr} is in {vals}')
    #     if type(vals) == ColEl:
    #         tmp_no_dup = vals.relation.drop_duplicates([vals.field]).rename(f'no_dup_{vals.field}')
    #         tmp_col_el = tmp_no_dup[vals.field]
    #         return IsinExpr(self, tmp_col_el)
    #
    #     if type(vals) == list or type(vals) == tuple:
    #         if len(vals) == 0:
    #             raise ValueError()
    #         if len(vals) == 1:
    #             return vals[0]
    #
    #         tmp_list = []
    #         for i in vals:
    #             if type(i) == str:
    #                 i = f'"{i}"'
    #             if ext:
    #                 tmp_list.append(CondExpr(unit1=ext, operator=CompareSymbol.EQ, unit2=i))
    #             else:
    #                 tmp_list.append(CondExpr(unit1=self, operator=CompareSymbol.EQ, unit2=i))
    #
    #         a = tmp_list.pop()
    #         b = tmp_list.pop()
    #         tmp_cond = a | b
    #         if tmp_list:
    #             for i in tmp_list:
    #                 tmp_cond |= i
    #         # print(tmp_cond)
    #         return tmp_cond

    @property
    def dt(self):
        self.data_type = 'date'
        return self

    @property
    def str(self):
        self.data_type = 'str'
        return self

    @property
    def date(self):
        self.data_type = 'date'
        return self

    @property
    def int(self):
        self.data_type = 'int'
        return self

    @property
    def real(self):
        self.data_type = 'real'
        return self

    def startswith(self, pattern: str):
        # A%
        self.add_const(pattern)
        return ExternalExpr(self, ExtFuncSymbol.StartsWith, self.get_const_var(pattern))
        # return ExternalExpr(self, 'StrStartsWith', pattern)

    def endswith(self, pattern: str):
        # %B
        return ExternalExpr(self, 'StrEndsWith', pattern)

    def contains(self, pattern):
        # %A%
        self.add_const(pattern)
        return ExternalExpr(self, ExtFuncSymbol.StringContains, self.get_const_var(pattern))
        # return ExternalExpr(self, 'StrContains', args)

    def contains_in_order(self, *args):
        # %A%B%
        return ExternalExpr(self, 'StrContains_in_order', args)

    def not_contains(self, *args):
        return ExternalExpr(self, 'not_StrContains', args)

    def substring(self, start, end):
        # substring
        return ExternalExpr(self, 'SubString', (start, end))

    def slice(self, start, end):
        # substring
        return ExternalExpr(self, 'SubString', (start, end))

    def find(self, pattern):
        self.add_const(pattern)
        return ExternalExpr(self, ExtFuncSymbol.FirstIndex, self.get_const_var(pattern))
        # return ExternalExpr(self, 'StrIndexOf', (pattern, start))

    def exists(self, bind_on, cond=None):
        return ExistExpr(self, bind_on, conds=cond)

    def not_exists(self, bind_on, cond=None):
        return ExistExpr(self, bind_on, conds=cond, reverse=True)

    def promote(self, func):
        self.follow_promotion = f'promote[{func}]'
        return self

    def sum(self):
        aggr_expr = AggrExpr(aggr_type=AggrType.VAL,
                             aggr_on=self.relation,
                             aggr_op={self.field: self.sdql_ir},
                             aggr_else=ConstantExpr(0.0))

        op_expr = OpExpr(op_obj=aggr_expr,
                         op_on=self.relation,
                         op_iter=True,
                         iter_on=self.relation,
                         ret_type=OpRetType.FLOAT)

        self.relation.push(op_expr)

        return aggr_expr

    def count(self):
        pass

    def mean(self):
        pass

    def min(self):
        pass

    def max(self):
        pass

    def replace(self, rec, inplace=False, mapper=None):
        # print(f'try to replace col {self.sdql_ir} with {rec} as record')
        # print(f'get {RecAccessExpr(rec, self.field)}')

        if mapper:
            if isinstance(mapper, dict):
                for k in mapper.keys():
                    if self.field in k:
                        if inplace:
                            return mapper[k]
                        else:
                            return RecAccessExpr(mapper[k], self.field)
            else:
                raise TypeError(f'mapper must be a dict')

        if inplace:
            return rec

        return RecAccessExpr(rec, self.field)

    @property
    def sdql_ir(self):
        return self.relation.key_access(self.field)
