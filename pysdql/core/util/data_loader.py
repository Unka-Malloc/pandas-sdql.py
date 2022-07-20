import os

from pysdql.core.dtypes.api import (
    relation,
    sdict,
    srecord,
)


def load_tbl(file_path: str, col_names: list, col_types=None, name=None):
    from pysdql.core.dtypes.LoadExpr import LoadExpr
    if len(col_names) != len(col_types):
        raise ValueError(f'length of names = {len(col_names)}, '
                         f'length of types = {len(col_types)}')
    return relation(name=name, data=LoadExpr(col_names, col_types, file_path), cols=col_names)


def read_tbl(path: str, header: list, col_types=None, name=None, sep='|', by_load=True):
    if '.tbl' not in path:
        raise TypeError()

    if name is None:
        name = str(os.path.basename(path)).removesuffix('.tbl')

    if by_load:
        if col_types is None:
            from pysdql.core.util.data_parser import get_tbl_type
            col_types = get_tbl_type(path, sep)
        return load_tbl(file_path=path,
                        col_names=header,
                        col_types=col_types,
                        name=name)

    with open(path, 'r') as tbl:
        line = tbl.readline()
        count = 0
        while line:
            count += 1
            # operation start

            # remove '\n'
            line_list = line.split(sep)

            if line[-1] == '\n':
                del line_list[-1]

            if not len(header) == len(line_list):
                raise ValueError(f'Incorrect number of columns: \n'
                                 f'length of header = {len(header)} \n'
                                 f'length of data = {len(line_list)} \n'
                                 f'in line {count}: {line}')

            # create a dictionary
            rec = srecord(dict(zip(header, line_list)))

            # operation end

            line = tbl.readline()
        else:
            return relation(name=name,
                            data=sdict({rec: 1}, name),
                            cols=header)


def tune_tbl(file_path, sep='|', name=None):
    if name is None:
        name = str(os.path.basename(file_path)).removesuffix('.tbl')

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
