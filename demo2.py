import pysdql

if __name__ == '__main__':
    # fix after refactor: 2, 11, 16, 20
    # fix after refactor hyper opt: 20
    pysdql.tpch_query(20, verbose=True, optimize=True)
