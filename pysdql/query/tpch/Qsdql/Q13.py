from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(cu, ord):

    # Insert
    special = "special"
    requests = "requests"
    orders_part = ord.sum(lambda x_orders: ({x_orders[0].o_custkey: record({"c_count": 1.0})}) if (((firstIndex(x_orders[0].o_comment, special) != -1) * (firstIndex(x_orders[0].o_comment, requests) > ((firstIndex(x_orders[0].o_comment, special)) + (6)))) == False) else (None))
    
    customer_aggr = cu.sum(lambda x_customer: {record({"c_count": (orders_part[x_customer[0].c_custkey].c_count) if (orders_part[x_customer[0].c_custkey] != None) else (0.0)}): 1.0})
    
    results = customer_aggr.sum(lambda x_customer_aggr: {record({"c_count": x_customer_aggr[0].c_count, "custdist": x_customer_aggr[1]}): True})
    
    # Complete

    return results
