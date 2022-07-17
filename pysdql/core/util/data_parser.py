import os
from pysdql.core.util.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str,
)


def get_tbl_type(file_path, sep='|'):
    name = str(os.path.basename(file_path)).removesuffix('.tbl')
    output = []
    with open(file_path, 'r') as file:
        line = file.readline()
        line = file.readline()

        # remove '\n'
        line_list = line.split(sep)

        if line[-1] == '\n':
            del line_list[-1]

        for i in line_list:
            if is_int(i):
                output.append('int')
            if is_float(i):
                output.append('real')
            if is_date(i):
                output.append('date')
            if is_str(i):
                output.append('string')

        # print(f'data: {line_list}\n'
        #       f'output: {output}')
        return output


def get_load(file_path, cols, name='', sep='|'):
    path = r'datasets/tuned/'
    file_name = str(os.path.basename(file_path))
    if not name:
        name = file_name.removesuffix('.tbl')

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
