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
