import pysdql

if __name__ == '__main__':
    test_all = pysdql.tpch_query(4, verbose=False, optimize=True)
