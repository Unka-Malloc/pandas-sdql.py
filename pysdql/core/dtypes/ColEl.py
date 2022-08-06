from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.ExistExpr import ExistExpr
from pysdql.core.dtypes.ValExpr import ValExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.IsinExpr import IsinExpr
from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.CondStmt import CondStmt


class ColEl:
    def __init__(self, dataframe, field: str, promoted=None):
        """
        ColUnit 在被创建的时候总是作为Relation的元素出现，因此必定存在IterExpr
        :param dataframe:
        :param field:
        """
        self.dataframe = dataframe
        self.field = field
        self.promoted = promoted
        self.follow_promotion = None
        self.data_type = ''

        self.isvar = False
        self.var_name = ''

    @property
    def year(self):
        return ExternalExpr(col=self,
                            ext_func='Year')

    @property
    def month(self):
        return ExternalExpr(col=self,
                            ext_func='Month')

    @property
    def day(self):
        return ExternalExpr(col=self,
                            ext_func='Day')

    def new_expr(self, new_str) -> str:
        if self.isvar:
            if self.promoted:
                f'promote[real]({self.var_name})'
            return f'{self.var_name}'
        return f'{new_str}.{self.field}'

    @property
    def from_1DT(self):
        if self.dataframe.type == '1DT':
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
        if self.dataframe.type == 'LRT':
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
        if self.dataframe.type == 'GRP':
            return True
        else:
            return False

    @property
    def record(self):
        if self.from_1DT:
            return self.dataframe.key
        if self.from_LRT:
            if self.field in self.dataframe.left.columns:
                return f'{self.dataframe.key}.left'
            if self.field in self.dataframe.right.columns:
                return f'{self.dataframe.key}.right'
        if self.from_GRP:
            if self.field in self.dataframe.groupby.columns:
                return self.dataframe.key

    @property
    def expr(self) -> str:
        return f'{self.dataframe.key}.{self.field}'
        # if self.isvar:
        #     if self.promoted:
        #         f'promote[real]({self.var_name})'
        #     return self.var_name
        # if self.from_LRtuple:
        #     if self.name in self.dataframe.left.cols:
        #         return f'{self.dataframe.key}.left.{self.name}'
        #     if self.name in self.dataframe.right.cols:
        #         return f'{self.dataframe.key}.right.{self.name}'
        #     else:
        #         raise ValueError()
        # if self.follow_promotion:
        #     return f'{self.follow_promotion}({self.dataframe.key}.{self.name})'
        # return f'{self.dataframe.key}.{self.name}'

    @property
    def sdql_expr(self) -> str:
        return ''

    def __str__(self):
        return self.sdql_expr

    def __repr__(self):
        return self.expr

    def __hash__(self):
        return hash((self.dataframe.key, self.dataframe.val, self.dataframe.name, self.field))

    def __lt__(self, other) -> CondExpr:
        """
        Less than
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        isjoin = False
        if type(other) == ColEl:
            isjoin = True
        return CondExpr(unit1=self, operator='<', unit2=other, isjoin=isjoin)
        # return f'{self.column} < {other}'

    def __le__(self, other) -> CondExpr:
        """
        Less than or Equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        isjoin = False
        if type(other) == ColEl:
            isjoin = True
        return CondExpr(unit1=self, operator='<=', unit2=other, isjoin=isjoin)
        # return f'{self.column} <= {other}'

    def __gt__(self, other) -> CondExpr:
        """
        Greater than
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        isjoin = False
        if type(other) == ColEl:
            isjoin = True
        return CondExpr(unit1=other, operator='<', unit2=self, isjoin=isjoin)
        # return f'{self.column} > {other}'

    def __ge__(self, other) -> CondExpr:
        """
        Greater than or Equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        isjoin = False
        if type(other) == ColEl:
            isjoin = True
        return CondExpr(unit1=other, operator='<=', unit2=self, isjoin=isjoin)
        # return f'{self.column} >= {other}'

    def __eq__(self, other) -> CondExpr:
        """
        Equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        isjoin = False
        if type(other) == ColEl:
            isjoin = True
        if type(other) == ColEl:
            if self.promoted:
                other.follow_promotion = self.promoted
        return CondExpr(unit1=self, operator='==', unit2=other, isjoin=isjoin)
        # return f'{self.column} == {other}'

    def __ne__(self, other) -> CondExpr:
        """
        Not equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        isjoin = False
        if type(other) == ColEl:
            isjoin = True
        return CondExpr(unit1=self, operator='!=', unit2=other, isjoin=isjoin)
        # return f'not ({self.column} == {other})'

    def __add__(self, other):
        return ColExpr(unit1=self, operator='+', unit2=other, inherit_from=self.dataframe)

    def __radd__(self, other):
        return ColExpr(unit1=other, operator='+', unit2=self, inherit_from=self.dataframe)

    def __sub__(self, other):
        return ColExpr(unit1=self, operator='-', unit2=other, inherit_from=self.dataframe)

    def __rsub__(self, other):
        return ColExpr(unit1=other, operator='-', unit2=self, inherit_from=self.dataframe)

    def __mul__(self, other):
        return ColExpr(unit1=self, operator='*', unit2=other, inherit_from=self.dataframe)

    def __rmul__(self, other):
        return ColExpr(unit1=other, operator='*', unit2=self, inherit_from=self.dataframe)

    def __truediv__(self, other):
        return ColExpr(unit1=self, operator='/', unit2=other, inherit_from=self.dataframe)

    def __rtruediv__(self, other):
        return ColExpr(unit1=other, operator='/', unit2=self, inherit_from=self.dataframe)

    def isin(self, vals, ext=None):
        # print(f'{self.expr} is in {vals}')
        if type(vals) == ColEl:
            tmp_no_dup = vals.dataframe.drop_duplicates([vals.field]).rename(f'no_dup_{vals.field}')
            tmp_col_el = tmp_no_dup[vals.field]
            return IsinExpr(self, tmp_col_el)

        if type(vals) == list or type(vals) == tuple:
            if len(vals) == 0:
                raise ValueError()
            if len(vals) == 1:
                return vals[0]

            tmp_list = []
            for i in vals:
                if type(i) == str:
                    i = f'"{i}"'
                if ext:
                    tmp_list.append(CondExpr(unit1=ext, operator='==', unit2=i))
                else:
                    tmp_list.append(CondExpr(unit1=self, operator='==', unit2=i))

            a = tmp_list.pop()
            b = tmp_list.pop()
            tmp_cond = a | b
            if tmp_list:
                for i in tmp_list:
                    tmp_cond |= i
            # print(tmp_cond)
            return tmp_cond

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
        return ExternalExpr(self, 'StrStartsWith', pattern)

    def endswith(self, pattern: str):
        # %B
        return ExternalExpr(self, 'StrEndsWith', pattern)

    def contains(self, *args):
        # %A%
        return ExternalExpr(self, 'StrContains', args)

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

    def find(self, pattern, start=0):
        return ExternalExpr(self, 'StrIndexOf', (pattern, start))

    # def exists(self, on, *args):
    #     return ExistExpr(self, on, conds=args)

    def exists(self, bind_on, cond=None):
        return ExistExpr(self, bind_on, conds=cond)

    def not_exists(self, bind_on, cond=None):
        return ExistExpr(self, bind_on, conds=cond, reverse=True)

    def promote(self, func):
        self.follow_promotion = f'promote[{func}]'
        return self

    def sum(self):
        tmp_name = f'{self.field}_sum'
        tmp_var = VarExpr(tmp_name, IterStmt(self.dataframe.iter_expr,
                                             f'{self.dataframe.key}.{self.field} * {self.dataframe.val}'))
        self.dataframe.push(OpExpr('colel_sum', tmp_var))

        return ValExpr(tmp_name, self.dataframe.operations)

        # tmp_name = f'{self.field}_sum'
        # tmp_var = VarExpr(tmp_name, IterStmt(self.dataframe.iter_expr, f'{self.dataframe.key}.{self.field} * {self.dataframe.val}'))
        # self.dataframe.history_name.append(tmp_name)
        # self.dataframe.operations.append(OpExpr('colel_sum', tmp_var))
        #
        # self.isvar = True
        # self.var_name = tmp_name
        #
        # return self

    def count(self):
        tmp_name = f'{self.field}_count'
        tmp_var = VarExpr(tmp_name, IterStmt(self.dataframe.iter_expr,
                                             f'{self.dataframe.val}'))
        self.dataframe.push(OpExpr('colel_count', tmp_var))

        return ValExpr(tmp_name, self.dataframe.operations)

    def mean(self):
        tmp_name = f'{self.field}_mean'
        tmp_rec = RecEl({f'{self.field}_sum': f'{self.dataframe.key}.{self.field} * {self.dataframe.val}',
                         f'{self.field}_count': f'{self.dataframe.val}'})
        tuple_var = VarExpr(f'{self.field}_sumcount', IterStmt(self.dataframe.iter_expr, f'{tmp_rec}'))
        tmp_var = VarExpr(tmp_name, f'{tuple_var} in {self.field}_sumcount.{self.field}_sum / {self.field}_sumcount.{self.field}_count')
        self.dataframe.push(OpExpr('colel_mean', tmp_var))

        return ValExpr(tmp_name, self.dataframe.operations)

        # tmp_name = f'{self.field}_mean'
        # tmp_rec = RecEl({f'{self.field}_sum': f'{self.dataframe.key}.{self.field} * {self.dataframe.val}',
        #                  f'{self.field}_count': f'{self.dataframe.val}'})
        # tuple_var = VarExpr(f'{self.field}_sumcount', IterStmt(self.dataframe.iter_expr, f'{tmp_rec}'))
        # tmp_var = VarExpr(tmp_name, f'{tuple_var} in {self.field}_sumcount.{self.field}_sum / {self.field}_sumcount.{self.field}_count')
        # self.dataframe.history_name.append(tmp_name)
        # self.dataframe.operations.append(OpExpr('colel_mean', tmp_var))
        #
        # self.isvar = True
        # self.var_name = tmp_name
        #
        # return self

    def min(self):
        tmp_name = f'{self.field}_min'
        tmp_iter = IterStmt(self.dataframe.iter_expr, f'promote[mnpr]({self.dataframe.key}.{self.field})')
        tmp_var = VarExpr(tmp_name, f'promote[real]({tmp_iter})')
        self.dataframe.push(OpExpr('colel_min', tmp_var))

        return ValExpr(tmp_name, self.dataframe.operations)

    def max(self):
        tmp_name = f'{self.field}_max'
        tmp_iter = IterStmt(self.dataframe.iter_expr, f'promote[mxpr]({self.dataframe.key}.{self.field})')
        tmp_var = VarExpr(tmp_name, f'promote[real]({tmp_iter})')
        self.dataframe.push(OpExpr('colel_max', tmp_var))

        return ValExpr(tmp_name, self.dataframe.operations)

        # tmp_name = f'{self.field}_max'
        # tmp_iter = IterStmt(self.dataframe.iter_expr, f'promote[mxpr]({self.dataframe.key}.{self.field})')
        # tmp_var = VarExpr(tmp_name, f'promote[real]({tmp_iter})')
        # self.dataframe.history_name.append(tmp_name)
        # self.dataframe.operations.append(OpExpr('colel_max', tmp_var))
        #
        # self.isvar = True
        # self.var_name = tmp_name
        #
        # return self
