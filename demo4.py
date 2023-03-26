import duckdb
import pandas as pd

from pysdql.query.tpch.const import (
    DATAPATH,
    LINEITEM_COLS,
    ORDERS_COLS,
    CUSTOMER_COLS,
    NATION_COLS,
    REGION_COLS,
    PART_COLS,
    SUPPLIER_COLS,
    PARTSUPP_COLS,

    LINEITEM_TYPE,
    ORDERS_TYPE,
    CUSTOMER_TYPE,
    NATION_TYPE,
    REGION_TYPE,
    PART_TYPE,
    SUPPLIER_TYPE,
    PARTSUPP_TYPE,
)

from pysdql.query.tpch.Qpostgres.postgresT import (
    tpch_q1,
)

from pysdql.query.tpch.Qduck import *

from pysdql.query.util import compare_dataframe, pandas_to_df

# show all columns
pd.set_option('display.max_columns', None)

if __name__ == '__main__':

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None,
                           names=LINEITEM_COLS,
                           dtype=LINEITEM_TYPE,
                           parse_dates=['l_shipdate', 'l_commitdate', 'l_receiptdate'])
    # orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
    #                      parse_dates=['o_orderdate'])
    # nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    # supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    # part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    # partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)

    pd_result = tpch_q1(lineitem)

    duck_conn = duckdb.connect(database=':memory:')

    duck_result = duck_conn.execute(duck_q1).df()

    compare_dataframe(pd_result, duck_result, verbose=True)

    print(pd_result.reset_index())

    print(duck_result)