from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    promo = "PROMO"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None))
    
    part_lineitem_probe = v0
    part_lineitem_part = pa
    build_side = part_lineitem_part.sum(lambda x: ({x[0].p_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = part_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_partkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"A": (((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) if (startsWith(x[0].p_type, promo)) else (0.0)})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"B": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: (record({"A": x[0].A, "B": x[0].B})) if (True) else (None))
    
    v4 = ((((v3.A) * (100.0))) / (v3.B))
    results = v4
    # Complete

    return results
