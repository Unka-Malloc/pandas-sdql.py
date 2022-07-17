import os

from pysdql.core.dtypes.api import (
    relation,
    sdict,
    srecord,
)


def read_tbl(path: str, header: list, name=None, sep='|'):
    if '.tbl' not in path:
        raise TypeError()

    if name is None:
        name = str(os.path.basename(path)).removesuffix('.tbl')

    data = {}

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
            data[rec] = 1

            # operation end

            line = tbl.readline()
        else:
            return relation(name=name,
                            data=sdict(data, name),
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
