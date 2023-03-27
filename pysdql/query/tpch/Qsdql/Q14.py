from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    promo = "PROMO"
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: True}) if (startsWith(x_part[0].p_type, promo)) else (None))
    
    part_lineitem = li.sum(lambda x_lineitem: (record({"A": (((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) if (part_part[x_lineitem[0].l_partkey] != None) else (0.0), "B": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})) if (((x_lineitem[0].l_shipdate >= 19950901) * (x_lineitem[0].l_shipdate < 19951001))) else (None))
    
    results = ((((part_lineitem.A) * (100.0))) / (part_lineitem.B))
    # Complete

    return results
