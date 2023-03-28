import pysdql

if __name__ == '__main__':
    # without reset cond psql fix: 7, 21, 22
    pysdql.tpch_query(range(1, 23), verbose=True, optimize=False)

    # fix squeeze: Q22 - GroupbyAggrFrame Optimize & Unoptimize
    # valid: 1, 3, 4, 5, 6, 8, 9, 10, 13, 14, 15, 16, 18, 19
    # error: 2, 7, 11, 17, 21, 22
    # fail: 12

    # pass: 4, 5, 9, 12, 13, 16
    # fail: 1
    # error: 2, 3, 6, 7, 8, 10, 11, 14, 15, 17, 18, 19, 20, 21, 22

    # unopt fail: 9, 15, 22