import os
from pysdql.core.util.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str,
)

from pysdql.core.util.data_str import remove_suffix


def get_dtype(names: list, data: list) -> dict:
    dtype = dict(zip(names, data))
    for k in dtype.keys():
        v = dtype[k]
        if is_int(v):
            dtype[k] = 'int'
        elif is_float(v):
            dtype[k] = 'real'
        elif is_date(v):
            dtype[k] = 'date'
        elif is_str(v):
            dtype[k] = 'string'
        else:
            raise ValueError(f'type ? ({k}, {v})')
    return dtype


def get_tbl_type(file_path, sep='|'):
    output = []
    with open(file_path, 'r') as file:
        line = file.readline()
        line = file.readline()

        # line = line.removesuffix('\n')
        line = remove_suffix(line, '\n')

        # remove '\n'
        line_list = line.split(sep)

        for i in line_list:
            if is_int(i):
                output.append('int')
            elif is_float(i):
                output.append('double')
            elif is_date(i):
                output.append('date')
            elif is_str(i):
                output.append('string')
            else:
                print(f'type ? {i}')

        return output


def get_load(file_path, cols, name='', sep='|'):
    path = r'datasets/tuned/'
    file_name = str(os.path.basename(file_path))
    if not name:
        # name = file_name.removesuffix('.tbl')
        name = remove_suffix(file_name, '.tbl')

    output = get_tbl_type(file_path, sep)

    if not len(cols) == len(output):
        raise ValueError(f'Incorrect number of columns: \n'
                         f'length of cols = {len(cols)} \n'
                         f'length of type = {len(output)} \n'
                         f'in line[2]')

    type_str = ''
    for i in range(len(cols)):
        type_str += f'{cols[i]}: {output[i]}'
        if i != len(cols) - 1:
            type_str += ', '
    result = f'let {name} = load[{{<{type_str}> -> int}}]("{path}{file_name}")'
    print(result)
    return result
