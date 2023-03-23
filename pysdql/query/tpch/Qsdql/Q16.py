from pysdql.query.tpch.const import (PARTSUPP_TYPE, PART_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ps": PARTSUPP_TYPE, "pa": PART_TYPE, "su": SUPPLIER_TYPE})
def query(ps, pa, su):
    # Insert
    brand45 = "Brand#45"
    mediumpolished = "MEDIUM POLISHED"
    customer = "Customer"
    complaints = "Complaints"
    v0 = su.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].s_comment, customer) != ((-1) * (1))) * (firstIndex(x[0].s_comment, complaints) > ((firstIndex(x[0].s_comment, customer)) + (7))))) else (None))
    
    partsupp_supplier_isin_build = v0
    partsupp_supplier_isin_build = partsupp_supplier_isin_build.sum(lambda x: {x[0].s_suppkey: True})
    
    v0 = ps.sum(lambda x: ({x[0]: x[1]}) if (partsupp_supplier_isin_build[x[0].ps_suppkey] == None) else (None))
    
    part_partsupp_probe = v0
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].p_brand != brand45) * (startsWith(x[0].p_type, mediumpolished) == False))) * (((((((((((((((x[0].p_size == 9) + (x[0].p_size == 36))) + (x[0].p_size == 49))) + (x[0].p_size == 14))) + (x[0].p_size == 23))) + (x[0].p_size == 45))) + (x[0].p_size == 19))) + (x[0].p_size == 3))))) else (None))
    
    part_partsupp_part = v0
    build_side = part_partsupp_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    v0 = part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_partkey] != None) else (None))
    
    v1 = v0.sum(lambda x: {record({"p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size}): record({"supplier_cnt": 1.0})})
    
    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})
    
    results = v2
    # Complete

    return results
