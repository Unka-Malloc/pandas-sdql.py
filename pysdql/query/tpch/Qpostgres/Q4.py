from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert
    v0 = li.sum(lambda x: (({x[0]: x[1]}) if (x[0].l_commitdate < x[0].l_receiptdate) else (None)) if (x[0] != None) else (None))
    
    orders_lineitem_probe = v0
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None)) if (x[0] != None) else (None))
    
    orders_lineitem_part = v0
    build_side = orders_lineitem_part.sum(lambda x: (({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = orders_lineitem_probe.sum(lambda x: (({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({record({"o_orderpriority": x[0].o_orderpriority}): record({"order_count": 1})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v2
    # Complete

    return results
