from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((((x[0].l_shipmode == ship) + (x[0].l_shipmode == mail))) * (x[0].l_commitdate < x[0].l_receiptdate))) * (x[0].l_shipdate < x[0].l_commitdate))) * (x[0].l_receiptdate >= 19940101))) * (x[0].l_receiptdate < 19950101))) else (None))
    
    orders_lineitem_probe = v0
    orders_lineitem_part = ord
    build_side = orders_lineitem_part.sum(lambda x: ({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"high_line_priority": (1) if (((x[0].o_orderpriority == urgent1) + (x[0].o_orderpriority == high2))) else (0)})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"low_line_priority": (1) if (((x[0].o_orderpriority != urgent1) * (x[0].o_orderpriority != high2))) else (0)})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: ({record({"l_shipmode": x[0].l_shipmode}): record({"high_line_count": x[0].high_line_priority, "low_line_count": x[0].low_line_priority})}) if (True) else (None))
    
    v4 = v3.sum(lambda x: ({x[0].concat(x[1]): True}) if (True) else (None))
    
    results = v4
    # Complete

    return results
