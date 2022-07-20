from pysdql.core.util.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str,
)
from pysdql.core.dtypes.VarExpr import VarExpr


class LoadExpr:
    def __init__(self, col_names, col_types, file_path):
        self.names = col_names
        self.types = col_types
        self.path = file_path

        self.load_list = []

    @property
    def expr(self) -> str:
        tmp_str = ''
        for i in range(len(self.names)):
            tmp_str += f'{self.names[i]}: {self.types[i]}'
            if i != len(self.names) - 1:
                tmp_str += ', '
        output = f'load[{{<{tmp_str}> -> int}}]("{self.path}")'
        self.load_list.append(output)
        return output

    def __repr__(self):
        return self.expr

    def __add__(self, other):
        self.load_list.append(other)
        return self

    def __iadd__(self, other):
        self.load_list.append(other)
        return self
