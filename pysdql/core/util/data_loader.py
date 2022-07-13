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

        while line:
            # operation start

            # remove '\n'
            line = line.split(sep)

            if line[-1] == '\n':
                del line[-1]

            # len(header) == len(line)
            if not len(header) == len(line):
                raise ValueError(f'Incorrect number of columns: \n'
                                 f'length of header = {len(header)} \n'
                                 f'length of data = {len(line)}')

            # create a dictionary
            rec = srecord(dict(zip(header, line)))
            data[rec] = 1

            # operation end

            line = tbl.readline()
        else:
            return relation(name=name,
                            data=sdict(data, name),
                            cols=header)
