from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "ps1": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, ps1, na, re):

    # Insert
    partsupp_supplier_build_nest_dict = ps.sum(lambda x: {x[0].ps_suppkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_build_pre_ops = su.sum(lambda x: (partsupp_supplier_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    nation_region_probe_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    nation_region_build_nest_dict = na.sum(lambda x: {x[0].n_regionkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_probe_pre_ops = nation_region_probe_pre_ops.sum(lambda x: (nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    partsupp_supplier_nation_region_build_nest_dict = partsupp_supplier_nation_region_build_pre_ops.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_part_build_pre_ops = partsupp_supplier_nation_region_probe_pre_ops.sum(lambda x: (partsupp_supplier_nation_region_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_region_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    partsupp_supplier_nation_region_part_probe_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (((x[0].p_size == 15) * (endsWith(x[0].p_type, brass)))) else (None))
    
    partsupp_supplier_nation_region_part_build_nest_dict = partsupp_supplier_nation_region_part_build_pre_ops.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_build_pre_ops = partsupp_supplier_nation_region_part_probe_pre_ops.sum(lambda x: (partsupp_supplier_nation_region_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_region_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    partsupp_supplier_nation_build_pre_ops = su.sum(lambda x: (partsupp_supplier_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    partsupp_supplier_nation_build_nest_dict = partsupp_supplier_nation_build_pre_ops.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_0 = partsupp_supplier_nation_region_probe_pre_ops.sum(lambda x: (partsupp_supplier_nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    partsupp_supplier_nation_region_1 = partsupp_supplier_nation_region_0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"min_ps_supplycost": x[0].ps_supplycost})})
    
    partsupp_supplier_nation_region_2 = partsupp_supplier_nation_region_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    partsupp_supplier_nation_region_3 = partsupp_supplier_nation_region_2.sum(lambda x: {x[0].concat(record({"minps_supplycost": x[0].min_ps_supplycost})): x[1]})
    
    partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_probe_pre_ops = partsupp_supplier_nation_region_3.sum(lambda x: {x[0]: x[1]})
    
    partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_build_nest_dict = partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_0 = partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_probe_pre_ops.sum(lambda x: (partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_build_nest_dict[x[0].ps_partkey] != None) else (None))
    
    partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_attach_to_df_aggr_1 = partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_supplycost == x[0].minps_supplycost) else (None))
    
    results = partsupp_supplier_nation_region_part_partsupp_supplier_nation_region_attach_to_df_aggr_1.sum(lambda x: {record({"s_acctbal": x[0].s_acctbal, "s_name": x[0].s_name, "n_name": x[0].n_name, "p_partkey": x[0].p_partkey, "p_mfgr": x[0].p_mfgr, "s_address": x[0].s_address, "s_phone": x[0].s_phone, "s_comment": x[0].s_comment}): True})
    
    # Complete

    return results
