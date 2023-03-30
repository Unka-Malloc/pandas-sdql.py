from typing import List
from pysdql.core.utils.data_parser import pandas_dtype_to_sdql
from pysdql.extlib.sdqlpy.sdql_lib import read_csv as sdql_read_csv


class LoadExpr:
    def __init__(self, filepath_or_buffer: str, sep: str, names: List[str], dtype: dict, parse_dates: List[str]):
        self.file_path = filepath_or_buffer
        self.names = names
        self.dtype = dtype
        self.parse_dates = parse_dates
        self.sep = sep

    def to_sdql_dtypes(self):
        return pandas_dtype_to_sdql(self.names, self.dtype, self.parse_dates)

    def to_dtype_str(self):
        final_list = []

        for c in self.names:
            if c in self.dtype.keys():
                t = self.dtype[c]

                if t == str:
                    final_list.append(f'"{c}": string(256)')
                elif t == bool:
                    final_list.append(f'"{c}": bool')
                elif t == int:
                    final_list.append(f'"{c}": int')
                elif t == float:
                    final_list.append(f'"{c}": float')
                else:
                    raise TypeError(f'Unexpected Type {t}')
            elif c in self.parse_dates:
                final_list.append(f'"{c}": date')
            else:
                raise IndexError(f'Type of {c} not found.')

        final_list.append(f'"_NA": string(1)')

        all_dtype_str = f"{{record({{{', '.join(final_list)}}}): bool}}"

        return all_dtype_str

    def to_sdql(self):
        return sdql_read_csv(file_path=self.file_path,
                             header_type_dict=self.to_sdql_dtypes(),
                             dataset_name='',
                             delimiter=self.sep)
