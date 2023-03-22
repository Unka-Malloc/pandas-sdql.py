from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, 'l1': LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, l1, pa):

    # Insert
    brand11 = "Brand#11"
    wrapcase = "WRAP CASE"
    part_l1_lineitem_probe = li
    v0 = l1.sum(lambda x: ({record({"l_partkey": x[0].l_partkey}): record({"sum_quant": x[0].l_quantity, "count_quant": 1})}) if (True) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(x[1]): True}) if (True) else (None))
    
    part_l1_probe = v1
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((x[0].p_brand == brand11) * (x[0].p_container == wrapcase))) else (None))
    
    part_l1_part = v0
    build_side = part_l1_part.sum(lambda x: ({x[0].p_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = part_l1_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_partkey] != None) else (None))
    
    part_l1_lineitem_part = v0
    build_side = part_l1_lineitem_part.sum(lambda x: ({x[0].l_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = part_l1_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_partkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"l_extendedprice": (x[0].l_extendedprice) if (x[0].l_quantity < ((0.2) * (((x[0].sum_quant) / (x[0].count_quant))))) else (0.0)})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: (record({"l_extendedprice": x[0].l_extendedprice})) if (True) else (None))
    
    v3 = ((v2.l_extendedprice) / (7.0))
    results = v3
    # Complete

    return results
