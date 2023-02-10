import pysdql
from pysdql.query.tpch.template import tpch_q8

if __name__ == '__main__':
    # test_all = pysdql.tpch_query([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
    #                              0, 1, verbose=False)

    # test_some = pysdql.tpch_query([2, 16, 20, 21], verbose=True)

    test_some = pysdql.tpch_query(6, 1, 1, verbose=True)