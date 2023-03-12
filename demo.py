import pysdql

if __name__ == '__main__':
    results = pysdql.tpch_query(range(1, 23), verbose=False, optimize=True)

    # results = pysdql.tpch_query([14, 17], verbose=False, optimize=True)

    # result = pysdql.tpch_query(14, verbose=True, optimize=True, mode='postgres')

    # pysdql.tpch_query(14, verbose=True, optimize=True)

