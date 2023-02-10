from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    v0 = ord.sum(lambda x: ({li.sum(lambda y: (({x[0]: True}) if (x[0].o_orderkey == y[0].l_orderkey) else (None)) if (y[0] != None) else (None))
    : True}) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (x[0] != None) else (None)) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None))
    
    v2 = v1.sum(lambda x: ({record({"o_orderpriority": x[0].o_orderpriority}): record({"order_count": 1})}) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: ({x[0].concat(x[1]): True}) if (x[0] != None) else (None))
    
    results = v3
    # Complete

    return results
