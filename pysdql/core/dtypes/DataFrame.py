from pysdql.core.dtypes.DataFrameColumns import DataFrameColumns
from pysdql.core.dtypes.IterEl import IterEl
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.OpSeq import OpSeq
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.SemiRing import SemiRing


class DataFrame(SemiRing):
    def __init__(self, data=None, index=None, columns=None, name=None, mutable=True, operations=OpSeq()):
        self.__default_name = 'R'
        self.__data = data
        self.__index = index
        self.__columns = columns
        self.__name = name
        self.__operations = operations

        self.operations.push(OpExpr('', self.variable))

    @property
    def mutable(self):
        if self.__data:
            return False
        return True

    @property
    def variable(self):
        if self.__data:
            return VarExpr(self.name, self.__data)
        return VarExpr(self.name, DictEl({}))

    @property
    def operations(self):
        return self.__operations

    @property
    def columns(self):
        if self.__columns:
            return DataFrameColumns(self, self.__columns)
        else:
            if self.__data:
                self.__columns = list(self.__data.keys())
                return DataFrameColumns(self, self.__columns)
            else:
                return DataFrameColumns(self, [])

    @property
    def name(self):
        if self.__name:
            return self.__name
        return self.__default_name

    @name.setter
    def name(self, val):
        allow_set_name = False
        if self.mutable:
            allow_set_name = True
        else:
            if self.name == self.__default_name:
                allow_set_name = True

        if allow_set_name:
            self.operations.push(OpExpr('', VarExpr(val, self.name)))
            self.__name = val

    @property
    def data(self):
        if self.__columns:
            columns_names = self.__columns
        else:
            columns_names = list(self.__data.keys())

        data_size = len(self.__data[columns_names[0]])

        rec_dict = {}
        for i in range(data_size):
            tmp_dict = {}
            for k in columns_names:
                tmp_dict[k] = self.__data[k][i]
            rec_dict[RecEl(tmp_dict)] = 1
        return DictEl(rec_dict)

    @property
    def iter_expr(self):
        return IterExpr(self.name)

    @property
    def sdql_expr(self):
        return self.operations.expr

    @property
    def expr(self) -> str:
        if self.name:
            return self.name
        return self.data.expr

    def __repr__(self):
        return self.expr

    def pop(self):
        self.operations.pop()

    def push(self, val):
        self.operations.push(val)
