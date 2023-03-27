import pysdql

if __name__ == '__main__':
    # unopt fix after refactor: 11
    pysdql.tpch_query(11, verbose=False, optimize=False)
