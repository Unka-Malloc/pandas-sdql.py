from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(cu, ord):

    # Insert
    special = "special"
    requests = "requests"
    orders_customer_probe = cu
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (((firstIndex(x[0].o_comment, special) != -1) * (firstIndex(x[0].o_comment, requests) > ((firstIndex(x[0].o_comment, special)) + (6)))) == False) else (None)) if (x[0] != None) else (None))
    
    orders_customer_part = v0
    build_side = orders_customer_part.sum(lambda x: (({x[0].o_custkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = orders_customer_probe.sum(lambda x: (({build_side[x[0].c_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].c_custkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({record({"c_custkey": x[0].c_custkey}): record({"c_count": 1})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({record({"c_count": x[0].c_count}): record({"custdist": 1})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v4 = v3.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v4
    # Complete

    return results
