from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    promo = "PROMO"
    lineitem_part_probe = pa
    v0 = li.sum(lambda x: (({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None)) if (x[0] != None) else (None))
    
    lineitem_part_part = v0
    build_side = lineitem_part_part.sum(lambda x: (({x[0].l_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = lineitem_part_probe.sum(lambda x: (({build_side[x[0].p_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].p_partkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0].concat(record({"case_a": ((((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))) if (True) else (0)) if (startsWith(x[0].p_type, promo)) else (0)})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v2_1 = v1.sum(lambda x: ((x[0].case_a if (True) else (None)) if (x[0] != None) else (None)))

    v2_2 = v1.sum(lambda x: (((x[0].l_extendedprice) * (((1) - (x[0].l_discount)) if (True) else (None)) if (x[0] != None) else (None))))
    
    v3 = ((((100.0) * (v2_1))) / (((v2_2))))
    results = v3
    # Complete

    return results
