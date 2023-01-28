from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    lineitem_part = lineitem_part.sum(lambda x_lineitem: ({x_lineitem[0].l_orderkey: True}) if (x_lineitem[0].l_commitdate < x_lineitem[0].l_receiptdate) else (None))
    
    orders_aggr = ord.sum(lambda x_orders: (({x_orders[0].o_orderpriority: 1}) if (lineitem_part[x_orders[0].o_orderkey] != None) else (None)) if (((x_orders[0].o_orderdate >= 19930701) * (x_orders[0].o_orderdate < 19931001))) else (None))
    
    results = orders_aggr.sum(lambda x_orders_aggr: {record({"o_orderpriority": x_orders_aggr[0], "order_count": x_orders_aggr[1]}): True})
    
    # Complete

    return results
