from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.SemiRing import SemiRing


class DataFrame(SemiRing):
    def __init__(self, data=None, index=None, columns=None):
        self.__data = data
        self.__index = index
        if columns:
            self.__columns = columns
        else:
            if data:
                self.__columns = list(data.keys())
        self.__columns = columns
        self.__name = ''

    @property
    def columns(self):
        return self

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, val):
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
