import os

import pysdql

from pysdql.core.dtypes.LoadExpr import LoadExpr

from pysdql.core.util.data_parser import get_dtype

from pysdql.core.util.data_str import (
    remove_prefix,
    remove_suffix,
    remove_sides
)

from pysdql.core.util.type_checker import is_header

from pysdql.const import (
    CUSTOMER_COLS,
    LINEITEM_COLS,
    ORDERS_COLS,
    NATION_COLS,
    REGION_COLS,
    PART_COLS,
    SUPPLIER_COLS,
    PARTSUPP_COLS
)

def read_table(filepath_or_buffer, sep=',', header=None, names=None, index_col=False, dtype=None, load=True):
    return read_csv(filepath_or_buffer=filepath_or_buffer,
                    sep=sep,
                    header=header,
                    names=names,
                    index_col=index_col,
                    dtype=dtype,
                    load=load)


def read_csv(filepath_or_buffer, sep=',', header=None, names=None, index_col=False, dtype=None, load=True):
    if names is None:
        names = []
    if dtype is None:
        dtype = {}
    if index_col:
        raise ValueError(f'Invalid index_col = {index_col}')

    file_name = str(os.path.basename(filepath_or_buffer))
    obj_name = file_name[:file_name.index('.')]

    with open(filepath_or_buffer, encoding='utf-8') as file:
        # remove '\n'
        line = file.readline()
        line_list = remove_suffix(line, '\n').split(sep)

        if header is None:
            if not names:
                if is_header(line_list):
                    names = line_list
                else:
                    names = [f'col{i}' for i in range(len(line_list))]
        elif header == 0:
            names = line_list
        else:
            raise ValueError(f'Invalid header = {header}')

        line = file.readline()
        line_list = remove_suffix(line, '\n').split(sep)

        dtype = get_dtype(names, line_list)

        return pysdql.DataFrame(data=LoadExpr(dtype, filepath_or_buffer), columns=names, dtype=dtype, name=obj_name)


def tune_tbl(file_path, sep='|', name=None):
    if name is None:
        name = remove_suffix(str(os.path.basename(file_path)), '.tbl')

    output_list = []

    parent_path = os.path.dirname(file_path)

    new_path = parent_path + os.sep + 'tuned'

    new_file_path = new_path + os.sep + name + '.tbl'

    if not os.path.exists(new_path):
        os.mkdir(new_path)

    with open(file_path, 'r') as file:
        line = file.readline()
        count = 0
        while line:
            count += 1
            # operation start

            # remove '\n'
            line_list = line.split(sep)

            if line[-1] == '\n':
                del line_list[-1]

            output = '|'.join(line_list)
            output_list.append(output)

            print(f'read {name}.tbl line[{count}]: {output}')

            line = file.readline()

    with open(new_file_path, 'w') as new_file:
        count = 0
        for line in output_list:
            count += 1
            new_file.write(line + '\n')
            print(f'write line[{count}]: {line}')
