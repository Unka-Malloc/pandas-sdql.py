from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    lineitem_orders_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((((((x[0].l_shipdate < 19950101) * (x[0].l_receiptdate >= 19940101))) * (x[0].l_receiptdate < 19950101))) * (x[0].l_commitdate < 19950101))) * (x[0].l_commitdate < x[0].l_receiptdate))) * (x[0].l_shipdate < x[0].l_commitdate))) * (((x[0].l_shipmode == ship) + (x[0].l_shipmode == mail))))) else (None))
    
    lineitem_orders_build_nest_dict = lineitem_orders_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_0 = ord.sum(lambda x: (lineitem_orders_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_orders_1 = lineitem_orders_0.sum(lambda x: {x[0].concat(record({"case_a": (1) if (((x[0].o_orderpriority == urgent1) + (x[0].o_orderpriority == high2))) else (0)})): x[1]})
    
    lineitem_orders_2 = lineitem_orders_1.sum(lambda x: {x[0].concat(record({"case_b": (1) if (((x[0].o_orderpriority != urgent1) * (x[0].o_orderpriority != high2))) else (0)})): x[1]})
    
    lineitem_orders_3 = lineitem_orders_2.sum(lambda x: {record({"l_shipmode": x[0].l_shipmode}): record({"high_line_count": x[0].case_a, "low_line_count": x[0].case_b})})
    
    lineitem_orders_4 = lineitem_orders_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_5 = lineitem_orders_4.sum(lambda x: {x[0]: {record({"high_line_count": x[0].high_line_count, "low_line_count": x[0].low_line_count}): True}})
    
    results = lineitem_orders_5.sum(lambda x: x[1])
    
    # Complete

    return results
