from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    isin_build = li.sum(lambda x: (({x[0].l_orderkey: True}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = ord.sum(lambda x: ((({x[0]: True}) if (isin_build[x[0].o_orderkey] != None) else (None)) if (True) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({record({"o_orderpriority": x[0].o_orderpriority}): record({"order_count": 1})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v3
    # Complete

    return results
