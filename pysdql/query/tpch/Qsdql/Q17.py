from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    brand23 = "Brand#23"
    medbox = "MED BOX"
    part_lineitem_index = pa.sum(lambda x: ({x[0]: x[1]}) if (((x[0].p_brand == brand23) * (x[0].p_container == medbox))) else (None))
    
    l1_0 = li.sum(lambda x: {record({"l_partkey": x[0].l_partkey}): record({"sum_quant": x[0].l_quantity, "count_quant": 1.0})})
    
    part_lineitem_probe = l1_0.sum(lambda x: {x[0].concat(x[1]): True})
    
    part_lineitem_build_nest_dict = part_lineitem_index.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    part_lineitem_lineitem_index = part_lineitem_probe.sum(lambda x: (part_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))
    
    part_lineitem_lineitem_build_nest_dict = part_lineitem_lineitem_index.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    part_lineitem_lineitem_0 = li.sum(lambda x: (part_lineitem_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_lineitem_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))
    
    part_lineitem_lineitem_1 = part_lineitem_lineitem_0.sum(lambda x: {x[0].concat(record({"l_extendedprice": (x[0].l_extendedprice) if (x[0].l_quantity < ((0.2) * (((x[0].sum_quant) / (x[0].count_quant))))) else (0.0)})): x[1]})
    
    part_lineitem_lineitem_2 = part_lineitem_lineitem_1.sum(lambda x: record({"l_extendedprice": x[0].l_extendedprice}))
    
    results = ((part_lineitem_lineitem_2.l_extendedprice) / (7.0))
    # Complete

    return results
