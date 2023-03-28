import pysdql

if __name__ == '__main__':
    # without reset cond psql fix: 7, 21, 22
    pysdql.tpch_query(21, verbose=True, optimize=False, mode='postgres')

    # fix squeeze: Q22 - GroupbyAggrFrame Optimize & Unoptimize
    # valid: 1, 3, 4, 5, 6, 8, 9, 10, 13, 14, 15, 16, 18, 19
    # error: 2, 7, 11, 17, 21, 22
    # fail: 12