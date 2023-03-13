import pysdql

if __name__ == '__main__':
    unoptimized = pysdql.tpch_query([8, 12, 14, 17], verbose=False, optimize=False)
