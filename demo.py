import pysdql

if __name__ == '__main__':
    results = pysdql.tpch_query(range(1, 23), verbose=True, optimize=True)
    