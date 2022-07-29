from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.ExistExpr import ExistExpr
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
    def __init__(self, relation, col_name: str, promoted=None):
        """
        ColUnit 在被创建的时候总是作为Relation的元素出现，因此必定存在IterExpr
        :param relation:
        :param col_name:
        """
        self.relation = relation
        self.name = col_name
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
            return f'{self.var_name}'
        return f'{new_str}.{self.name}'

    @property
    def from_1Dtuple(self):
        from pysdql.core.dtypes.relation import relation
        if type(self.relation) == relation:
            return True
        else:
            return False

    @property
    def from_LRtuple(self):
        from pysdql.core.dtypes.JoinExpr import JoinExpr
        if type(self.relation) == JoinExpr:
            return True
        else:
            return False

    @property
    def expr(self):
        if self.isvar:
            return self.var_name
        if self.from_LRtuple:
            if self.name in self.relation.left.cols:
                return f'{self.relation.key}.left.{self.name}'
            if self.name in self.relation.right.cols:
                return f'{self.relation.key}.right.{self.name}'
            else:
                raise ValueError()
        if self.follow_promotion:
            return f'{self.follow_promotion}({self.relation.key}.{self.name})'
        return f'{self.relation.key}.{self.name}'

    def __repr__(self):
        return self.expr

    def __hash__(self):
        return hash((self.relation.key, self.name))

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
        return ColExpr(unit1=self, operator='+', unit2=other, inherit_from=self.relation)

    def __radd__(self, other):
        return ColExpr(unit1=other, operator='+', unit2=self, inherit_from=self.relation)

    def __sub__(self, other):
        return ColExpr(unit1=self, operator='-', unit2=other, inherit_from=self.relation)

    def __rsub__(self, other):
        return ColExpr(unit1=other, operator='-', unit2=self, inherit_from=self.relation)

    def __mul__(self, other):
        return ColExpr(unit1=self, operator='*', unit2=other, inherit_from=self.relation)

    def __rmul__(self, other):
        return ColExpr(unit1=other, operator='*', unit2=self, inherit_from=self.relation)

    def __truediv__(self, other):
        return ColExpr(unit1=self, operator='/', unit2=other, inherit_from=self.relation)

    def __rtruediv__(self, other):
        return ColExpr(unit1=other, operator='/', unit2=self, inherit_from=self.relation)

    def isin(self, vals, ext=None):
        if type(vals) == ColEl:
            return IsinExpr(self, vals)

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
        tmp_name = f'{self.name}_sum'
        tmp_var = VarExpr(tmp_name, IterStmt(self.relation.iter_expr, f'{self.relation.key}.{self.name}'))
        self.relation.history_name.append(tmp_name)
        self.relation.operations.append(OpExpr('colel_sum', tmp_var))

        self.isvar = True
        self.var_name = tmp_name

        return self
