from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    building = "BUILDING"
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate > 19950315) else (None))
    
    lineitem_orders_customer_build_pre_ops = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    orders_0 = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderdate < 19950315) else (None))
    
    orders_customer_build_pre_ops = orders_0.sum(lambda x: {record({"o_orderkey": x[0].o_orderkey, "o_custkey": x[0].o_custkey, "o_orderstatus": x[0].o_orderstatus, "o_totalprice": x[0].o_totalprice, "o_orderdate": x[0].o_orderdate, "o_orderpriority": x[0].o_orderpriority, "o_clerk": x[0].o_clerk, "o_shippriority": x[0].o_shippriority, "o_comment": x[0].o_comment}): True})
    
    customer_0 = cu.sum(lambda x: ({x[0]: x[1]}) if (x[0].c_mktsegment == building) else (None))
    
    orders_customer_probe_pre_ops = customer_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey}): True})
    
    orders_customer_build_nest_dict = orders_customer_build_pre_ops.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_0 = orders_customer_probe_pre_ops.sum(lambda x: (orders_customer_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_customer_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_orders_customer_probe_pre_ops = orders_customer_0.sum(lambda x: {record({"o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority, "o_orderkey": x[0].o_orderkey}): True})
    
    lineitem_orders_customer_build_nest_dict = lineitem_orders_customer_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_0 = lineitem_orders_customer_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_orders_customer_1 = lineitem_orders_customer_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    lineitem_orders_customer_1 = lineitem_orders_customer_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    lineitem_orders_customer_1 = lineitem_orders_customer_0.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_orders_customer_2 = lineitem_orders_customer_1.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority}): record({"revenue": x[0].before_1})})
    
    lineitem_orders_customer_3 = lineitem_orders_customer_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_customer_4 = lineitem_orders_customer_3.sum(lambda x: {record({"revenue": x[0].revenue}): True})
    
    results = lineitem_orders_customer_4.sum(lambda x: {record({"revenue": x[0].revenue}): True})
    
    # Complete

    return results
