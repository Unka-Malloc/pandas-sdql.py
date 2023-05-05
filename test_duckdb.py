import time

import duckdb
import pandas as pd

from pysdql.query.tpch.Qduck import *

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

lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS, parse_dates=["l_shipdate"])
customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS, parse_dates=['o_orderdate'])
part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

if __name__ == '__main__':
    for i in range(22):
        all_time = []

        for j in range(10):
            duck_start_time = time.time()

            duck_result = duck_conn.execute(eval(f'duck_q{i + 1}')).df()

            duck_end_time = time.time()
            exec_time = (duck_end_time - duck_start_time) * 1000

            all_time.append(exec_time)

        print(f'Query {i + 1}: {sum(all_time) / 10} ms')