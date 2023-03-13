import pysdql

if __name__ == '__main__':
    results = pysdql.tpch_query(range(1, 23), verbose=False, optimize=True)

    # unoptimized = pysdql.tpch_query(range(1, 23), verbose=False, optimize=False)

    # unopt_errors = pysdql.tpch_query([11, 14, 15, 17, 18, 22], verbose=False, optimize=False)

    # pysdql.tpch_query(14, verbose=True, optimize=True)

    # unopt_single = pysdql.tpch_query(14, verbose=True, optimize=False)

    # verified = pysdql.tpch_query([1, 6, 12, 13, 14], verbose=True, optimize=True, mode='postgres')

    # waiting_for_unique = pysdql.tpch_query([3, 4, 5, 10, 16, 19], verbose=True, optimize=True, mode='postgres')

    # result = pysdql.tpch_query(2, verbose=True, optimize=True, mode='postgres')

    # pysdql.tpch_query(14, verbose=True, optimize=True)
