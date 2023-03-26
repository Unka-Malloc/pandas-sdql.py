from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(cu, ord):

    # Insert
    special = "special"
    requests = "requests"
    orders_customer_index = ord.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].o_comment, special) != -1) * (firstIndex(x[0].o_comment, requests) > ((firstIndex(x[0].o_comment, special)) + (6)))) == False) else (None))
    
    orders_customer_build_nest_dict = orders_customer_index.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_0 = cu.sum(lambda x: ({x[0]: True}) if (orders_customer_build_nest_dict[x[0].c_custkey] == None) else (orders_customer_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ))
    
    orders_customer_1 = orders_customer_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey}): record({"c_count": (1.0) if (x[0].o_orderkey != None) else (0.0)})})
    
    orders_customer_2 = orders_customer_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    orders_customer_3 = orders_customer_2.sum(lambda x: {record({"c_count": x[0].c_count}): record({"custdist": (1.0) if (x[0].c_custkey != None) else (0.0)})})
    
    results = orders_customer_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
