from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "ps1": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, ps1, na, re):

    # Insert
    brass = "BRASS"
    europe = "EUROPE"
    part_partsupp_supplier_nation_region_index = pa.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))
    
    nation_region_probe = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    nation_region_build_nest_dict = na.sum(lambda x: {x[0].n_regionkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_region_probe = nation_region_probe.sum(lambda x: (nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    supplier_nation_region_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_probe = supplier_nation_region_probe.sum(lambda x: (supplier_nation_region_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_region_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    partsupp_supplier_nation_region_build_nest_dict = ps.sum(lambda x: {x[0].ps_suppkey: sr_dict({x[0]: x[1]})})
    
    part_partsupp_supplier_nation_region_probe = partsupp_supplier_nation_region_probe.sum(lambda x: (partsupp_supplier_nation_region_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_region_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    part_partsupp_supplier_nation_region_build_nest_dict = part_partsupp_supplier_nation_region_index.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_index = part_partsupp_supplier_nation_region_probe.sum(lambda x: (part_partsupp_supplier_nation_region_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_partsupp_supplier_nation_region_build_nest_dict[x[0].ps_partkey] != None) else (None))
    
    part_partsupp_supplier_nation_region_index = pa.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))
    
    partsupp_part_probe = part_0.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))
    
    partsupp_part_build_nest_dict = partsupp_part_index.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})
    
    supplier_partsupp_part_probe = partsupp_part_probe.sum(lambda x: (partsupp_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    supplier_partsupp_part_build_nest_dict = supplier_partsupp_part_index.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    supplier_partsupp_part_nation_region_index = supplier_partsupp_part_probe.sum(lambda x: (supplier_partsupp_part_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_partsupp_part_build_nest_dict[x[0].ps_suppkey] != None) else (None))
    
    nation_region_probe = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    region_1 = region_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    nation_region_probe = region_2.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    nation_region_build_nest_dict = nation_region_index.sum(lambda x: {x[0].n_regionkey: sr_dict({x[0]: x[1]})})
    
    supplier_partsupp_part_nation_region_probe = nation_region_probe.sum(lambda x: (nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    supplier_partsupp_part_nation_region_build_nest_dict = supplier_partsupp_part_nation_region_index.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    supplier_partsupp_part_nation_region_0 = supplier_partsupp_part_nation_region_probe.sum(lambda x: (supplier_partsupp_part_nation_region_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_partsupp_part_nation_region_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    supplier_partsupp_part_nation_region_1 = supplier_partsupp_part_nation_region_0.sum(lambda x: {record({"p_partkey": x[0].p_partkey}): record({"min_ps_supplycost": x[0].ps_supplycost})})
    
    supplier_partsupp_part_nation_region_2 = supplier_partsupp_part_nation_region_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_probe = supplier_partsupp_part_nation_region_2.sum(lambda x: {x[0].concat(record({"minps_supplycost": x[0].min_ps_supplycost})): x[1]})
    
    part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_build_nest_dict = part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_index.sum(lambda x: {record({"ps_supplycost": x[0].ps_supplycost, "p_partkey": x[0].p_partkey}): sr_dict({x[0]: x[1]})})
    
    results = part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_probe.sum(lambda x: (part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_build_nest_dict[record({"minps_supplycost": x[0].minps_supplycost, "p_partkey": x[0].p_partkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_partsupp_supplier_nation_region_supplier_partsupp_part_nation_region_build_nest_dict[record({"minps_supplycost": x[0].minps_supplycost, "p_partkey": x[0].p_partkey})] != None) else (None))
    
    # Complete

    return results
