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
    tpch_q13,
)

from pysdql.query.tpch.Qpandas.pandasQ import *

from pysdql.query.util import compare_dataframe, pandas_to_df

# show all columns
pd.set_option('display.max_columns', None)

if __name__ == '__main__':

    # lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None,
    #                        names=LINEITEM_COLS,
    #                        dtype=LINEITEM_TYPE,
    #                        parse_dates=['l_shipdate', 'l_commitdate', 'l_receiptdate'])
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    # nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    # supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    # part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    # partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)

    # pd_result = tpch_q13(customer, orders)
    #
    # duck_conn = duckdb.connect(database=':memory:')
    #
    # duck_result = duck_conn.execute(duck_q13).df()
    #
    # compare_dataframe(pd_result, duck_result, verbose=True)
    #
    # print(duck_result)

    res1 = orders[~((orders['o_comment'].str.find('special') != -1)
                        & (orders['o_comment'].str.rfind('requests') > (orders['o_comment'].str.find('special') + 6)))]

    res2 = orders[~(orders.o_comment.str.contains("^.*?special.*?requests.*?$", regex=True))]

    print(res1.shape, res2.shape)

    # compare_dataframe(res1, res2, verbose=True)