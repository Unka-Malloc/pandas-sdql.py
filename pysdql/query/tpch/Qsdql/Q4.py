from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    lineitem_orders_isin_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_commitdate < x[0].l_receiptdate) else (None))
    
    lineitem_orders_isin_build_index = lineitem_orders_isin_pre_ops.sum(lambda x: {x[0].l_orderkey: True})
    
    orders_0 = ord.sum(lambda x: ({x[0]: x[1]}) if (lineitem_orders_isin_build_index[x[0].o_orderkey] != None) else (None))
    
    orders_1 = orders_0.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None))
    
    orders_2 = orders_1.sum(lambda x: {record({"o_orderpriority": x[0].o_orderpriority}): record({"order_count": (1.0) if (x[0].o_orderdate != None) else (0.0)})})
    
    results = orders_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
