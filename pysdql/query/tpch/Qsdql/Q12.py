from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_orderkey: record({"l_shipmode": x_lineitem[0].l_shipmode})}) if (((((((((((x_lineitem[0].l_shipmode == ship) + (x_lineitem[0].l_shipmode == mail))) * (x_lineitem[0].l_commitdate < x_lineitem[0].l_receiptdate))) * (x_lineitem[0].l_shipdate < x_lineitem[0].l_commitdate))) * (x_lineitem[0].l_receiptdate >= 19950101))) * (x_lineitem[0].l_receiptdate < 19960101))) else (None))
    
    orders_aggr = ord.sum(lambda x_orders: ({lineitem_part[x_orders[0].o_orderkey].l_shipmode: record({"high_line_count": (1) if (((x_orders[0].o_orderpriority == urgent1) + (x_orders[0].o_orderpriority == high2))) else (0), "low_line_count": (1) if (((x_orders[0].o_orderpriority != urgent1) * (x_orders[0].o_orderpriority != high2))) else (0)})}) if (lineitem_part[x_orders[0].o_orderkey] != None) else (None))
    
    results = orders_aggr.sum(lambda x_orders_aggr: {record({"l_shipmode": x_orders_aggr[0], "high_line_count": x_orders_aggr[1].high_line_count, "low_line_count": x_orders_aggr[1].low_line_count}): True})
    
    # Complete

    return results
