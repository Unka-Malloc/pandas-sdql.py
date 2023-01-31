import pysdql
from pysdql.query.tpch.template import tpch_q8

if __name__ == '__main__':
    test_all = pysdql.tpch_query([1, 3, 4, 5, 6, 7, 8, 10, 12, 14, 15, 16, 18, 19], 0, 1, verbose=True)
    print(f'Whether all queries are passed: {test_all}')

    # test_one = pysdql.tpch_query(12)

    # print(pysdql.query.tpch.Qpandas.q12())
