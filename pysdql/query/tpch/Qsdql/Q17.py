from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    brand23 = "Brand#23"
    medbox = "MED BOX"
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: True}) if (((x_part[0].p_brand == brand23) * (x_part[0].p_container == medbox))) else (None))
    
    part_l1 = li.sum(lambda x_l1: ({x_l1[0].l_partkey: record({"count_quant": 1.0, "sum_quant": x_l1[0].l_quantity})}) if (part_part[x_l1[0].l_partkey] != None) else (None))
    
    part_l1_lineitem = li.sum(lambda x_lineitem: record({"l_extendedprice": ((x_lineitem[0].l_extendedprice) if (x_lineitem[0].l_quantity < ((0.2) * (((part_l1[x_lineitem[0].l_partkey].sum_quant) / (part_l1[x_lineitem[0].l_partkey].count_quant))))) else (0.0)) if (part_l1[x_lineitem[0].l_partkey] != None) else (0.0)}))
    
    results = ((part_l1_lineitem.l_extendedprice) / (7.0))
    # Complete

    return results
