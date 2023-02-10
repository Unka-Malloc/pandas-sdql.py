from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    building = "BUILDING"
    v0 = cu.sum(lambda x: (({x[0]: x[1]}) if (x[0] != None) else (None)) if (x[0].c_mktsegment == building) else (None))
    
    v0_part = v0
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (x[0] != None) else (None)) if (x[0].o_orderdate < 19950315) else (None))
    
    v0_probe = v0
    v0 = v0_probe.sum(lambda x: ({v0_part.sum(lambda y: ((x[0].concat(y[0])) if (x[0].o_custkey == y[0].c_custkey) else (None)) if (y[0] != None) else (None))
    : True}) if (x[0] != None) else (None))
    
    v0_part = v0
    v0 = li.sum(lambda x: (({x[0]: x[1]}) if (x[0] != None) else (None)) if (x[0].l_shipdate > 19950315) else (None))
    
    v0_probe = v0
    v0 = v0_probe.sum(lambda x: ({v0_part.sum(lambda y: ((x[0].concat(y[0])) if (x[0].l_orderkey == y[0].o_orderkey) else (None)) if (y[0] != None) else (None))
    : True}) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: ({record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate, "o_shippriority": x[0].o_shippriority}): record({"revenue": x[0].revenue})}) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: ({x[0].concat(x[1]): True}) if (x[0] != None) else (None))
    
    results = v3
    # Complete

    return results
