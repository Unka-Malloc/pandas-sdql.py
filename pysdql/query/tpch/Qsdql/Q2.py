from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, na, re):

    # Insert
    europe = "EUROPE"
    brass = "BRASS"
    region_nation_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    region_nation_build_nest_dict = region_nation_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_build_pre_ops = na.sum(lambda x: (region_nation_build_nest_dict[x[0].n_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_build_nest_dict[x[0].n_regionkey] != None) else (None))
    
    region_nation_supplier_build_nest_dict = region_nation_supplier_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_pre_ops = su.sum(lambda x: (region_nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    region_nation_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    region_nation_build_nest_dict = region_nation_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_build_pre_ops = na.sum(lambda x: (region_nation_build_nest_dict[x[0].n_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_build_nest_dict[x[0].n_regionkey] != None) else (None))
    
    region_nation_supplier_build_nest_dict = region_nation_supplier_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_ps1_build_pre_ops = su.sum(lambda x: (region_nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    region_nation_supplier_ps1_build_nest_dict = region_nation_supplier_ps1_build_pre_ops.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_ps1_0 = ps.sum(lambda x: (region_nation_supplier_ps1_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_supplier_ps1_build_nest_dict[x[0].ps_suppkey] != None) else (None))
    
    region_nation_supplier_ps1_1 = region_nation_supplier_ps1_0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"ps_supplycost": x[0].ps_supplycost})})
    
    region_nation_supplier_ps1_2 = region_nation_supplier_ps1_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    region_nation_supplier_ps1_part_partsupp_build_pre_ops = region_nation_supplier_ps1_2.sum(lambda x: {x[0].concat(record({"min_supplycost": x[0].ps_supplycost})): x[1]})
    
    part_partsupp_build_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))
    
    part_partsupp_build_nest_dict = part_partsupp_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_ps1_part_partsupp_probe_pre_ops = ps.sum(lambda x: (part_partsupp_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))
    
    region_nation_supplier_ps1_part_partsupp_build_nest_dict = region_nation_supplier_ps1_part_partsupp_build_pre_ops.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe_pre_ops = region_nation_supplier_ps1_part_partsupp_probe_pre_ops.sum(lambda x: (region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_pre_ops.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_0 = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe_pre_ops.sum(lambda x: (region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_suppkey] != None) else (None))
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_1 = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_supplycost == x[0].min_supplycost) else (None))
    
    results = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_1.sum(lambda x: {record({"s_acctbal": x[0].s_acctbal, "s_name": x[0].s_name, "n_name": x[0].n_name, "p_partkey": x[0].p_partkey, "p_mfgr": x[0].p_mfgr, "s_address": x[0].s_address, "s_phone": x[0].s_phone, "s_comment": x[0].s_comment}): True})
    
    # Complete

    return results
