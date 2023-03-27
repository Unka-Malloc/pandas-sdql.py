import pysdql

if __name__ == '__main__':
    # unopt fix after refactor: 11
    pysdql.tpch_query(range(1, 23), verbose=False, optimize=True)
