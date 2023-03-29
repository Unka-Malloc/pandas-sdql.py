from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    orders_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((((x[0].l_shipmode == ship) + (x[0].l_shipmode == mail))) * (x[0].l_commitdate < x[0].l_receiptdate))) * (x[0].l_shipdate < x[0].l_commitdate))) * (x[0].l_receiptdate >= 19940101))) * (x[0].l_receiptdate < 19950101))) else (None))
    
    orders_lineitem_build_nest_dict = ord.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_0 = orders_lineitem_probe_pre_ops.sum(lambda x: (orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    orders_lineitem_1 = orders_lineitem_0.sum(lambda x: {x[0].concat(record({"case_a": (1) if (((x[0].o_orderpriority == urgent1) + (x[0].o_orderpriority == high2))) else (0)})): x[1]})
    
    orders_lineitem_2 = orders_lineitem_1.sum(lambda x: {x[0].concat(record({"case_b": (1) if (((x[0].o_orderpriority != urgent1) * (x[0].o_orderpriority != high2))) else (0)})): x[1]})
    
    orders_lineitem_3 = orders_lineitem_2.sum(lambda x: {record({"l_shipmode": x[0].l_shipmode}): record({"high_line_count": x[0].case_a, "low_line_count": x[0].case_b})})
    
    orders_lineitem_4 = orders_lineitem_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    orders_lineitem_5 = orders_lineitem_4.sum(lambda x: {x[0]: {record({"high_line_count": x[0].high_line_count, "low_line_count": x[0].low_line_count}): True}})
    
    results = orders_lineitem_5.sum(lambda x: x[1])
    
    # Complete

    return results
