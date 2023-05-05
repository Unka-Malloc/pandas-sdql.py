def benchmark_pandas():
    import time

    import pandas as pd

    from pysdql.query.tpch.template import (
        tpch_q1,
        tpch_q2,
        tpch_q3,
        tpch_q4,
        tpch_q5,
        tpch_q6,
        tpch_q7,
        tpch_q8,
        tpch_q9,
        tpch_q10,
        tpch_q11,
        tpch_q12,
        tpch_q13,
        tpch_q14,
        tpch_q15,
        tpch_q16,
        tpch_q17,
        tpch_q18,
        tpch_q19,
        tpch_q20,
        tpch_q21,
        tpch_q22,
    )

    from pysdql.query.tpch.const import (
        DATAPATH,
        LINEITEM_COLS,
        ORDERS_COLS,
        CUSTOMER_COLS,
        NATION_COLS,
        REGION_COLS,
        PART_COLS,
        SUPPLIER_COLS,
        PARTSUPP_COLS
    )

    pd.set_option('mode.chained_assignment', None)

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS,
                           parse_dates=["l_shipdate"])
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    pd_query_ = {
        1: 'tpch_q1(lineitem)',
        2: 'tpch_q2(part, supplier, partsupp, nation, region)',
        3: 'tpch_q3(lineitem, customer, orders)',
        4: 'tpch_q4(orders, lineitem)',
        5: 'tpch_q5(lineitem, customer, orders, region, nation, supplier)',
        6: 'tpch_q6(lineitem)',
        7: 'tpch_q7(supplier, lineitem, orders, customer, nation)',
        8: 'tpch_q8(part, supplier, lineitem, orders, customer, nation, region)',
        9: 'tpch_q9(lineitem, orders, nation, supplier, part, partsupp)',
        10: 'tpch_q10(customer, orders, lineitem, nation)',
        11: 'tpch_q11(partsupp, supplier, nation)',
        12: 'tpch_q12(orders, lineitem)',
        13: 'tpch_q13(customer, orders)',
        14: 'tpch_q14(lineitem, part)',
        15: 'tpch_q15(lineitem, supplier)',
        16: 'tpch_q16(partsupp, part, supplier)',
        17: 'tpch_q17(lineitem, part)',
        18: 'tpch_q18(lineitem, customer, orders)',
        19: 'tpch_q19(lineitem, part)',
        20: 'tpch_q20(supplier, nation, partsupp, part, lineitem)',
        21: 'tpch_q21(supplier, lineitem, orders, nation)',
        22: 'tpch_q22(customer, orders)',
    }

    res = []

    for k in pd_query_.keys():
        all_time = []

        for i in range(10):
            start_time = time.time()

            eval(pd_query_[k])

            end_time = time.time()
            exec_time = (end_time - start_time) * 1000

            all_time.append(exec_time)

        print(f'Query {k}: {sum(all_time) / 10} ms')

        res.append(sum(all_time) / 10)

    return res

def benchmark_duckdb():
    import time

    import duckdb
    import pandas as pd

    from pysdql.query.tpch.Qduck import (
        duck_q1,
        duck_q2,
        duck_q3,
        duck_q4,
        duck_q5,
        duck_q6,
        duck_q7,
        duck_q8,
        duck_q9,
        duck_q10,
        duck_q11,
        duck_q12,
        duck_q13,
        duck_q14,
        duck_q15,
        duck_q16,
        duck_q17,
        duck_q18,
        duck_q19,
        duck_q20,
        duck_q21,
        duck_q22,
    )

    from pysdql.query.tpch.const import (
        DATAPATH,
        LINEITEM_COLS,
        ORDERS_COLS,
        CUSTOMER_COLS,
        NATION_COLS,
        REGION_COLS,
        PART_COLS,
        SUPPLIER_COLS,
        PARTSUPP_COLS
    )

    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS,
                           parse_dates=["l_shipdate"])
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    res = []

    for i in range(22):
        all_time = []

        for j in range(10):
            duck_start_time = time.time()

            duck_conn.execute(eval(f'duck_q{i + 1}')).df()

            duck_end_time = time.time()
            exec_time = (duck_end_time - duck_start_time) * 1000

            all_time.append(exec_time)

        print(f'Query {i + 1}: {sum(all_time) / 10} ms')

        res.append(sum(all_time) / 10)

    return res

def benchmark_sdql(start=1, end=22, mode='python', optimize=True):
    import time

    from pysdql.query.tpch.Qsdql.benchmark_sdql import (
        q1,
        q2,
        q3,
        q4,
        q5,
        q6,
        q7,
        q8,
        q9,
        q10,
        q11,
        q12,
        q13,
        q14,
        q15,
        q16,
        q17,
        q18,
        q19,
        q20,
        q21,
        q22,
    )

    res = []

    for i in range(start, end + 1):
        print(f'Query {i} | ->')

        exec_time =  eval(f'q{i}(optimize={optimize})')

        res.append(exec_time)

    return res
