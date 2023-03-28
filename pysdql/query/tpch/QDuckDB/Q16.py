from pysdql.query.tpch.const import (PARTSUPP_TYPE, PART_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ps": PARTSUPP_TYPE, "pa": PART_TYPE, "su": SUPPLIER_TYPE})
def query(ps, pa, su):
    # Insert
    brand45 = "Brand#45"
    mediumpolished = "MEDIUM POLISHED"
    customer = "Customer"
    complaints = "Complaints"
    partsupp_part_probe_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (((((((((((((((((((x[0].p_size == 9) + (x[0].p_size == 36))) + (x[0].p_size == 49))) + (x[0].p_size == 14))) + (x[0].p_size == 23))) + (x[0].p_size == 45))) + (x[0].p_size == 19))) + (x[0].p_size == 3))) * (x[0].p_brand != brand45))) * (startsWith(x[0].p_type, mediumpolished) == False))) else (None))
    
    partsupp_part_build_nest_dict = ps.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})
    
    partsupp_part_0 = partsupp_part_probe_pre_ops.sum(lambda x: (partsupp_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    supplier_partsupp_part_isin_pre_ops = su.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].s_comment, customer) != ((-1) * (1))) * (firstIndex(x[0].s_comment, complaints) > ((firstIndex(x[0].s_comment, customer)) + (7))))) else (None))
    
    supplier_partsupp_part_isin_build_index = supplier_partsupp_part_isin_pre_ops.sum(lambda x: {x[0].s_suppkey: True})
    
    partsupp_part_1 = partsupp_part_0.sum(lambda x: ({x[0]: x[1]}) if (supplier_partsupp_part_isin_build_index[x[0].ps_suppkey] == None) else (None))
    
    partsupp_part_2 = partsupp_part_1.sum(lambda x: {record({"p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size}): record({"supplier_cnt": sr_dict({x[0].ps_suppkey: True})})})
    
    partsupp_part_3 = partsupp_part_2.sum(lambda x: {record({"p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size, "supplier_cnt": dictSize(x[1].supplier_cnt)}): True})
    
    results = partsupp_part_3.sum(lambda x: {record({"supplier_cnt": x[0].supplier_cnt}): True})
    
    # Complete

    return results
