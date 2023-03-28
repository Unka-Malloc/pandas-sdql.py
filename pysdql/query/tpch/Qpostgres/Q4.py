from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    orders_0 = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None))
    
    orders_lineitem_build_pre_ops = orders_0.sum(lambda x: {record({"o_orderkey": x[0].o_orderkey, "o_custkey": x[0].o_custkey, "o_orderstatus": x[0].o_orderstatus, "o_totalprice": x[0].o_totalprice, "o_orderdate": x[0].o_orderdate, "o_orderpriority": x[0].o_orderpriority, "o_clerk": x[0].o_clerk, "o_shippriority": x[0].o_shippriority, "o_comment": x[0].o_comment}): True})
    
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_commitdate < x[0].l_receiptdate) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): True})
    
    orders_lineitem_probe_pre_ops = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): True})
    
    orders_lineitem_build_nest_dict = orders_lineitem_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_0 = orders_lineitem_probe_pre_ops.sum(lambda x: (orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    orders_lineitem_1 = orders_lineitem_0.sum(lambda x: {record({"o_orderpriority": x[0].o_orderpriority}): True})
    
    orders_lineitem_1 = orders_lineitem_0.sum(lambda x: {record({"o_orderpriority": x[0].o_orderpriority}): True})
    
    orders_lineitem_1 = orders_lineitem_0.sum(lambda x: {record({"o_orderpriority": x[0].o_orderpriority}): record({"order_count": (1.0) if (x[0].o_orderpriority != None) else (0.0)})})
    
    orders_lineitem_2 = orders_lineitem_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    results = orders_lineitem_2.sum(lambda x: {record({"order_count": x[0].order_count}): True})
    
    # Complete

    return results
