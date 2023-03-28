import pysdql

if __name__ == '__main__':
    # without reset cond psql fix: 7, 8, 21, 22
    pysdql.tpch_query(range(1, 23), verbose=True, optimize=False, mode='postgres')

    # fix squeeze: Q22 - GroupbyAggrFrame Optimize & Unoptimize
    # fix