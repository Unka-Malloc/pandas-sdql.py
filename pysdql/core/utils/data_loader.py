import os

from varname import varname

import pysdql

from pysdql.core.exprs.carrier.LoadExpr import LoadExpr

from pysdql.core.utils.data_str import (
    remove_suffix,
)


def read_csv(filepath_or_buffer, sep=',', header=None, names=None, index_col=False, dtype=None, parse_dates=None):
    if names is None:
        names = []
    if dtype is None:
        dtype = {}
    if index_col:
        raise ValueError(f'Invalid index_col = {index_col}')

    obj_name = varname()

    load_expr = LoadExpr(filepath_or_buffer=filepath_or_buffer, sep=sep, names=names, dtype=dtype, parse_dates=parse_dates)

    return pysdql.DataFrame(data=load_expr,
                            columns=names,
                            dtype=load_expr.to_sdql_dtypes(),
                            name=obj_name,
                            loader=load_expr)

def read_table(filepath_or_buffer, sep='|', header=None, names=None, index_col=False, dtype=None, parse_dates=None):
    if names is None:
        names = []
    if dtype is None:
        dtype = {}
    if index_col:
        raise ValueError(f'Invalid index_col = {index_col}')

    obj_name = varname()

    load_expr = LoadExpr(filepath_or_buffer=filepath_or_buffer, sep=sep, names=names, dtype=dtype, parse_dates=parse_dates)

    return pysdql.DataFrame(data=load_expr,
                            columns=names,
                            dtype=load_expr.to_sdql_dtypes(),
                            name=obj_name,
                            loader=load_expr)


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
