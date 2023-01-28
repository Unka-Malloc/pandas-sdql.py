from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    building = "BUILDING"
    customer_part = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: True}) if (x_customer[0].c_mktsegment == building) else (None))
    
    customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record({"o_orderdate": x_orders[0].o_orderdate, "o_orderkey": x_orders[0].o_orderkey, "o_shippriority": x_orders[0].o_shippriority})}) if (customer_part[x_orders[0].o_custkey] != None) else (None)) if (x_orders[0].o_orderdate < 19950315) else (None))
    
    lineitem_aggr = li.sum(lambda x_lineitem: (({record({"l_orderkey": x_lineitem[0].l_orderkey, "o_orderdate": customer_orders[x_lineitem[0].l_orderkey].o_orderdate, "o_shippriority": customer_orders[x_lineitem[0].l_orderkey].o_shippriority}): ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))}) if (customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (x_lineitem[0].l_shipdate > 19950315) else (None))
    
    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record({"l_orderkey": x_lineitem_aggr[0].l_orderkey, "o_orderdate": x_lineitem_aggr[0].o_orderdate, "o_shippriority": x_lineitem_aggr[0].o_shippriority, "revenue": x_lineitem_aggr[1]}): True})
    
    # Complete

    return results
