import pysdql
import pandas as pd
from pysdql.query.tpch.const import DATAPATH, LINEITEM_COLS

if __name__ == '__main__':
    test_all = pysdql.tpch_query([1, 3, 4, 6, 14, 15, 16, 18], 0, 1)

    # old_lines = []
    #
    # with open(f'T:/UG4-Proj/pysdql/query/tpch/Qsdql/Q6.py', 'r') as f:
    #     for line in f:
    #         old_lines.append(line)
    #
    # first_index = old_lines.index('    # Insert\n')
    #
    # second_index = old_lines.index('    # Complete\n')
    #
    # first_lines = old_lines[:first_index + 1]
    #
    # second_lines = old_lines[second_index:]
    #
    # new_lines = first_lines + second_lines
    #
    # print(new_lines)

    # print(pysdql.query.tpch.Qsdql.q3())


