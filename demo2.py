import time

import pysdql

if __name__ == '__main__':
    start_time = time.time()

    # psql fix: 7, 21

    pysdql.tpch_query(range(1, 23), verbose=True, optimize=False)

    # fix squeeze: Q22 - GroupbyAggrFrame Optimize & Unoptimize
    # valid: 1, 3, 4, 5, 6, 8, 9, 10, 13, 14, 15, 16, 18, 19
    # error: 2, 7, 11, 17, 21, 22
    # fail: 12

    # pass: 1, 4, 5, 9, 12, 13, 16
    # fail: 18
    # error: 2, 3, 6, 7, 8, 10, 11, 14, 15, 17, 19, 20, 21, 22

    # unopt fail: 9, 15, 22

    # unopt: pass except 22
    # postgres: pass except 11

    # orders_lineitem_supplier_nation_lineitem_supplier_nation_lineitem_probe_pre_ops None
    # lineitem_supplier_nation_lineitem_lineitem_supplier_nation_probe_pre_ops

    end_time = time.time()

    print((end_time - start_time) / 60)