from pysdql.query.tpch.const import (PARTSUPP_TYPE, PART_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ps": PARTSUPP_TYPE, "pa": PART_TYPE, "su": SUPPLIER_TYPE})
def query(ps, pa, su):
    # Insert
    brand45 = "Brand#45"
    mediumpolished = "MEDIUM POLISHED"
    customer = "Customer"
    complaints = "Complaints"
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: record({"p_partkey": x_part[0].p_partkey, "p_brand": x_part[0].p_brand, "p_type": x_part[0].p_type, "p_size": x_part[0].p_size})}) if (((((x_part[0].p_brand != brand45) * (startsWith(x_part[0].p_type, mediumpolished) == False))) * (((((((((((((((x_part[0].p_size == 9) + (x_part[0].p_size == 36))) + (x_part[0].p_size == 49))) + (x_part[0].p_size == 14))) + (x_part[0].p_size == 23))) + (x_part[0].p_size == 45))) + (x_part[0].p_size == 19))) + (x_part[0].p_size == 3))))) else (None))
    
    supplier_part = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: True}) if (((firstIndex(x_supplier[0].s_comment, customer) != ((-1) * (1))) * (firstIndex(x_supplier[0].s_comment, complaints) > ((firstIndex(x_supplier[0].s_comment, customer)) + (7))))) else (None))
    
    partsupp_aggr = ps.sum(lambda x_partsupp: (({record({"p_brand": part_part[x_partsupp[0].ps_partkey].p_brand, "p_type": part_part[x_partsupp[0].ps_partkey].p_type, "p_size": part_part[x_partsupp[0].ps_partkey].p_size}): 1}) if (supplier_part[x_partsupp[0].ps_suppkey] == None) else (None)) if (part_part[x_partsupp[0].ps_partkey] != None) else (None))
    
    results = partsupp_aggr.sum(lambda x_partsupp_aggr: {record({"p_brand": x_partsupp_aggr[0].p_brand, "p_type": x_partsupp_aggr[0].p_type, "p_size": x_partsupp_aggr[0].p_size, "supplier_cnt": x_partsupp_aggr[1]}): True})
    
    # Complete

    return results
