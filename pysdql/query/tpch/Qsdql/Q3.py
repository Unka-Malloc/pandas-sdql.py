from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    building = "BUILDING"
    customer_orders_build_pre_ops = cu.sum(lambda x: ({x[0]: x[1]}) if (x[0].c_mktsegment == building) else (None))
    
    customer_orders_probe_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderdate < 19950315) else (None))
    
    customer_orders_build_nest_dict = customer_orders_build_pre_ops.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    customer_orders_lineitem_build_pre_ops = customer_orders_probe_pre_ops.sum(lambda x: (customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))
    
    customer_orders_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate > 19950315) else (None))
    
    customer_orders_lineitem_build_nest_dict = customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    customer_orders_lineitem_0 = customer_orders_lineitem_probe_pre_ops.sum(lambda x: (customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    customer_orders_lineitem_1 = customer_orders_lineitem_0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    customer_orders_lineitem_2 = customer_orders_lineitem_1.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority}): record({"revenue": x[0].revenue})})
    
    results = customer_orders_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
