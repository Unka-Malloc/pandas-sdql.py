from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    promo = "PROMO"
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_partkey: record({"l_extendedprice": x_lineitem[0].l_extendedprice, "l_discount": x_lineitem[0].l_discount, "l_partkey": x_lineitem[0].l_partkey})}) if (((x_lineitem[0].l_shipdate >= 19950901) * (x_lineitem[0].l_shipdate < 19951001))) else (None))
    
    lineitem_part = pa.sum(lambda x_part: record({"case_a": ((((lineitem_part[x_part[0].p_partkey].l_extendedprice) * (((1) - (lineitem_part[x_part[0].p_partkey].l_discount))))) if (lineitem_part[x_part[0].p_partkey] != None) else (0)) if (startsWith(x_part[0].p_type, promo)) else (0), "l_extendedprice": unique((lineitem_part[x_part[0].p_partkey].l_extendedprice) if (lineitem_part[x_part[0].p_partkey] != None) else (0.0)), "l_discount": unique((lineitem_part[x_part[0].p_partkey].l_discount) if (lineitem_part[x_part[0].p_partkey] != None) else (0.0))}))
    
    results = ((((100.0) * (lineitem_part.case_a))) / (((lineitem_part.l_extendedprice) * (((1) - (lineitem_part.l_discount))))))
    # Complete

    return results
