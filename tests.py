import pysdql

if __name__ == '__main__':
    # test_all = pysdql.tpch_query([1, 3, 4, 5, 6, 10, 14, 15, 16, 18, 19], 0, 1, verbose=True)
    # print(f'Whether all queries are passed: {test_all}')

    test_one = pysdql.tpch_query(5)
