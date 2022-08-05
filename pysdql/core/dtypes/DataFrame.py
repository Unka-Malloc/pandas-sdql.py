from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.DataFrameColumns import DataFrameColumns
from pysdql.core.dtypes.DataFrameStruct import DataFrameStruct
from pysdql.core.dtypes.IterEl import IterEl
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.OpSeq import OpSeq
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.SemiRing import SemiRing

from pysdql.core.util.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str
)


class DataFrame(SemiRing):
    def __init__(self, data=None, index=None, columns=None, dtype=None, name=None, operations=OpSeq()):
        self.__default_name = 'R'
        self.__data = data
        self.__index = index
        self.__columns = columns
        self.__dtype = dtype
        self.__name = name
        self.__operations = operations

        self.__structure = DataFrameStruct('1DT')

        self.operations.push(OpExpr('', self.variable))

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
    def index(self):
        return self.__index

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
    def dtype(self):
        if self.__dtype:
            return self.__dtype

        if self.__data:
            tmp_dict = {}
            for k in self.__data.keys():
                first_item = self.__data[k][0]
                if is_int(first_item):
                    tmp_dict[k] = 'int'
                elif is_float(first_item):
                    tmp_dict[k] = 'real'
                elif is_date(first_item):
                    tmp_dict[k] = 'date'
                elif is_str(first_item):
                    tmp_dict[k] = 'string'
                else:
                    raise ValueError(f'Cannot identify type {first_item}')
            return tmp_dict

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
    def operations(self):
        return self.__operations

    def pop(self):
        self.operations.pop()

    def push(self, val):
        self.operations.push(val)

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
    def iter_expr(self):
        return IterExpr(self.name)

    @property
    def key(self):
        return self.iter_expr.key

    @property
    def val(self):
        return self.iter_expr.val

    @property
    def expr(self) -> str:
        if self.name:
            return self.name
        return self.data.expr

    @property
    def sdql_expr(self):
        return self.operations.expr

    def __str__(self):
        return self.sdql_expr

    def __repr__(self):
        return self.expr

    @property
    def structure(self) -> str:
        return self.__structure.type

    @structure.setter
    def structure(self, val: str):
        self.__structure = DataFrameStruct(val)

    def __getitem__(self, item):
        if type(item) == str:
            return ColEl(self, item)
