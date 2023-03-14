import pysdql

if __name__ == '__main__':
    pysdql.tpch_query(20, verbose=True, optimize=False)
    # unoptimized = pysdql.tpch_query([15, 18, 22], verbose=False, optimize=False)
