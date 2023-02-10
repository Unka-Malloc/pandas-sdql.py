from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    building = "BUILDING"
    v0 = li.sum(lambda x: (({x[0]: x[1]}) if (x[0].l_shipdate > 19950315) else (None)) if (x[0] != None) else (None))
    
    customer_orders_lineitem_probe = v0
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (x[0].o_orderdate < 19950315) else (None)) if (x[0] != None) else (None))
    
    customer_orders_probe = v0
    v0 = cu.sum(lambda x: (({x[0]: x[1]}) if (x[0].c_mktsegment == building) else (None)) if (x[0] != None) else (None))
    
    customer_orders_part = v0
    build_side = customer_orders_part.sum(lambda x: (({x[0].c_custkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = customer_orders_probe.sum(lambda x: (({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_custkey] != None) else (None)) if (x[0] != None) else (None))
    
    customer_orders_lineitem_part = v0
    build_side = customer_orders_lineitem_part.sum(lambda x: (({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = customer_orders_lineitem_probe.sum(lambda x: (({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority}): record({"revenue": x[0].revenue})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v3
    # Complete

    return results
