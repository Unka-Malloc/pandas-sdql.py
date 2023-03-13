from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    orders_part = ord.sum(lambda x_orders: ({x_orders[0].o_orderkey: record({"o_orderkey": x_orders[0].o_orderkey, "o_custkey": x_orders[0].o_custkey, "o_orderstatus": x_orders[0].o_orderstatus, "o_totalprice": x_orders[0].o_totalprice, "o_orderdate": x_orders[0].o_orderdate, "o_orderpriority": x_orders[0].o_orderpriority, "o_clerk": x_orders[0].o_clerk, "o_shippriority": x_orders[0].o_shippriority, "o_comment": x_orders[0].o_comment})}) if (((x_orders[0].o_orderdate >= 19930701) * (x_orders[0].o_orderdate < 19931001))) else (None))
    
    lineitem_aggr = li.sum(lambda x_lineitem: (({orders_part[x_lineitem[0].l_orderkey].o_orderpriority: 1}) if (orders_part[x_lineitem[0].l_orderkey] != None) else (None)) if (x_lineitem[0].l_commitdate < x_lineitem[0].l_receiptdate) else (None))
    
    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record({"o_orderpriority": x_lineitem_aggr[0], "order_count": x_lineitem_aggr[1]}): True})
    
    # Complete

    return results
