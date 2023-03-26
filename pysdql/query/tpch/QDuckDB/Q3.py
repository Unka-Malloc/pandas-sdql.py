from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    df_aggr_1_0 = df_aggr_1.sum(lambda x: {x[0].concat(record({"l_orderkey": x[0].l_orderkey})): x[1]})
    
    df_aggr_1_1 = df_aggr_1_0.sum(lambda x: {x[0].concat(record({"revenue": x[0].revenue})): x[1]})
    
    df_aggr_1_2 = df_aggr_1_1.sum(lambda x: {x[0].concat(record({"o_orderdate": x[0].o_orderdate})): x[1]})
    
    results = df_aggr_1_2.sum(lambda x: {x[0].concat(record({"o_shippriority": x[0].o_shippriority})): x[1]})
    
    # Complete

    return results
