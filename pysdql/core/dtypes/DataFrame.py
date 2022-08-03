from pysdql.core.dtypes.DataFrameColumns import DataFrameColumns
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.OpSeq import OpSeq
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.SemiRing import SemiRing


class DataFrame(SemiRing):
    def __init__(self, data=None, index=None, columns=None, name=None, load=None, mutable=True, op_seq=None):
        self.__default_name = 'R'
        self.__data = data
        self.__index = index
        if columns:
            self.__columns = columns
        else:
            if data:
                self.__columns = list(data.keys())
            else:
                self.__columns = columns
        if name:
            self.__name = name
        else:
            self.__name = self.__default_name

        if op_seq:
            self.__op_seq = op_seq
        else:
            self.__op_seq = OpSeq()

        self.mutable = True

        if data:
            self.operations.push(OpExpr('', VarExpr(self.name, data)))
            self.mutable = False

        if load:
            self.operations.push(OpExpr('', VarExpr(self.name, load)))
            self.mutable = False

    @property
    def operations(self):
        return self.__op_seq

    @property
    def columns(self):
        return DataFrameColumns(self, self.__columns)

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, val):
        allow_set_name = True
        if not self.mutable:
            if self.__name != self.__default_name:
                allow_set_name = False

        if allow_set_name:
            self.operations.push(OpExpr('', VarExpr(val, self.__name)))
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
            for k in self.__columns:
                tmp_dict[k] = self.__data[k][i]
            rec_dict[RecEl(tmp_dict)] = 1
        return DictEl(rec_dict)

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
