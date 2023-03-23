from pysdql.query.tpch.const import (LINEITEM_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "su": SUPPLIER_TYPE})
def query(li, su):

    # Insert
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19960101) * (x[0].l_shipdate < 19960401))) else (None))
    
    v1 = v0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    v2 = v1.sum(lambda x: {record({"l_suppkey": x[0].l_suppkey}): record({"total_revenue": x[0].revenue})})
    
    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})
    
    v4 = v3.sum(lambda x: ({x[0]: x[1]}) if (x[0].total_revenue == 1614410.2928) else (None))
    
    supplier_lineitem_probe = v4
    supplier_lineitem_part = su
    build_side = supplier_lineitem_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    v0 = supplier_lineitem_probe.sum(lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_suppkey] != None) else (None))
    
    results = v0
    # Complete

    return results
