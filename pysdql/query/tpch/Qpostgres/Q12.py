from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    orders_part = ord.sum(lambda x_orders: {x_orders[0].o_orderkey: record({"o_orderkey": x_orders[0].o_orderkey, "o_custkey": x_orders[0].o_custkey, "o_orderstatus": x_orders[0].o_orderstatus, "o_totalprice": x_orders[0].o_totalprice, "o_orderdate": x_orders[0].o_orderdate, "o_orderpriority": x_orders[0].o_orderpriority, "o_clerk": x_orders[0].o_clerk, "o_shippriority": x_orders[0].o_shippriority, "o_comment": x_orders[0].o_comment})})
    
    lineitem_aggr = li.sum(lambda x_lineitem: (({x_lineitem[0].l_shipmode: record({"high_line_count": (1) if (((orders_part[x_lineitem[0].l_orderkey].o_orderpriority == urgent1) + (orders_part[x_lineitem[0].l_orderkey].o_orderpriority == high2))) else (0), "low_line_count": (1) if (((orders_part[x_lineitem[0].l_orderkey].o_orderpriority != urgent1) * (orders_part[x_lineitem[0].l_orderkey].o_orderpriority != high2))) else (0)})}) if (orders_part[x_lineitem[0].l_orderkey] != None) else (None)) if (((((((((((x_lineitem[0].l_shipmode == ship) + (x_lineitem[0].l_shipmode == mail))) * (x_lineitem[0].l_commitdate < x_lineitem[0].l_receiptdate))) * (x_lineitem[0].l_shipdate < x_lineitem[0].l_commitdate))) * (x_lineitem[0].l_receiptdate >= 19940101))) * (x_lineitem[0].l_receiptdate < 19950101))) else (None))
    
    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record({"l_shipmode": x_lineitem_aggr[0], "high_line_count": x_lineitem_aggr[1].high_line_count, "low_line_count": x_lineitem_aggr[1].low_line_count}): True})
    
    # Complete

    return results
