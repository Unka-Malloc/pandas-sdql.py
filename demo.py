import time

import pysdql

if __name__ == '__main__':
    start_time = time.time()

    optimized = pysdql.tpch_query(range(1, 23), verbose=True, optimize=True)

    # opt_part1 = pysdql.tpch_query(range(1, 11), verbose=True, optimize=True)

    # opt_part2 = pysdql.tpch_query(range(12, 23), verbose=True, optimize=True)

    # opt_single = pysdql.tpch_query(5, verbose=True, optimize=True)

    # unoptimized = pysdql.tpch_query(range(1, 23), verbose=True, optimize=False)

    # unopt_single = pysdql.tpch_query(21, verbose=True, optimize=False)

    # fix 11

    # psql = pysdql.tpch_query(list(range(1, 20)) + [22], verbose=True, optimize=False, mode='postgres')

    # psql_unopt = pysdql.tpch_query(18, verbose=True, optimize=False, mode='postgres')

    # with column projection everywhere:
    # fail: 1, 4, 9
    # error: 18

    # psql_unopt_verified = pysdql.tpch_query([1, 3, 4, 5, 6, 10, 12, 13, 14, 16, 19], verbose=True, optimize=False, mode='postgres')

    # psql unopt current progress: 1, 2, 3, 4, 5, 6, 10, 12, 13, 14, 16, 17, 18, 19, 20

    # psql unopt pass: 1, 2, 3, 4, 5, 6, 10, 12, 13, 14, 16, 17, 18, 19, 20
    # psql unopt failed:
    # psql unopt error: 7, 8, 9, 11, 15, 21, 22

    # duck_unopt = pysdql.tpch_query(range(1, 23), verbose=True, optimize=False, mode='duckdb')

    # duck_unopt_single = pysdql.tpch_query(16, verbose=True, optimize=False, mode='duckdb')

    # duck unopt pass: 1, 4, 5, 9, 13, 16
    # duck unopt fail: 18
    # duck unopt error: ...

    end_time = time.time()

    print((end_time - start_time) / 60)
