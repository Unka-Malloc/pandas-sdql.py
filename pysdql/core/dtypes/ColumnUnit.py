from pysdql.core.dtypes.ConditionalUnit import CondUnit
from pysdql.core.dtypes.ColumnExpr import ColExpr
from pysdql.core.dtypes.ConstructionExpr import ConstrExpr
from pysdql.core.dtypes.IsinExpr import IsinExpr
from pysdql.core.dtypes.IterationExpr import IterExpr


class ColUnit:
    def __init__(self, relation, col_name: str):
        """
        ColUnit 在被创建的时候总是作为Relation的元素出现，因此必定存在IterExpr
        :param relation:
        :param col_name:
        """
        self.relation = relation
        self.name = col_name

    def aggr(self, aggr_func=None, *args, **kwargs):
        """

        :param aggr_func:
        :param args:
        :param kwargs:
        :return: pysdql.Relation
        """
        return self.relation.aggr_on_col(col_name=self.name, aggr_func=aggr_func, args=args, kwargs=kwargs)

    def new_expr(self, new_str) -> str:
        return f'{new_str}.{self.name}'

    @property
    def expr(self):
        return f'{self.relation.iter_expr.key}.{self.name}'

    def __repr__(self):
        return self.expr

    def __hash__(self):
        return hash((self.relation.iter_expr.key, self.name))

    def __lt__(self, other) -> CondUnit:
        """
        Less than
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        return CondUnit(unit1=self, operator='<', unit2=other)
        # return f'{self.column} < {other}'

    def __le__(self, other) -> CondUnit:
        """
        Less than or Equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        return CondUnit(unit1=self, operator='<=', unit2=other)
        # return f'{self.column} <= {other}'

    def __gt__(self, other) -> CondUnit:
        """
        Greater than
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        return CondUnit(unit1=self, operator='>', unit2=other)
        # return f'{self.column} > {other}'

    def __ge__(self, other) -> CondUnit:
        """
        Greater than or Equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        return CondUnit(unit1=self, operator='>=', unit2=other)
        # return f'{self.column} >= {other}'

    def __eq__(self, other) -> CondUnit:
        """
        Equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        return CondUnit(unit1=self, operator='==', unit2=other)
        # return f'{self.column} == {other}'

    def __ne__(self, other) -> CondUnit:
        """
        Not equal
        :param other:
        :return:
        """
        if type(other) == str:
            other = f'"{other}"'
        return CondUnit(unit1=self, operator='!=', unit2=other)
        # return f'not ({self.column} == {other})'

    def __add__(self, other):
        return ColExpr(unit1=self, operator='+', unit2=other)

    def __radd__(self, other):
        return ColExpr(unit1=other, operator='+', unit2=self)

    def __sub__(self, other):
        return ColExpr(unit1=self, operator='-', unit2=other)

    def __rsub__(self, other):
        return ColExpr(unit1=other, operator='-', unit2=self)

    def __mul__(self, other):
        return ColExpr(unit1=self, operator='*', unit2=other)

    def __rmul__(self, other):
        return ColExpr(unit1=other, operator='*', unit2=self)

    def __truediv__(self, other):
        return ColExpr(unit1=self, operator='/', unit2=other)

    def __floordiv__(self, other):
        return ColExpr(unit1=self, operator='/', unit2=other)

    def __rfloordiv__(self, other):
        return ColExpr(unit1=other, operator='/', unit2=self)

    def isin(self, vals):
        if type(vals) == ColUnit:
            return IsinExpr(self, vals)

        if type(vals) == list or type(vals) == tuple:
            column = f'{self.relation.iter_expr.key}.{self.name}'

            tmp_list = []
            for i in vals:
                tmp_list.append(f'{column} == "{i}"')

            return f"({' || '.join(tmp_list)})"
