from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "ps1": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, ps1, na, re):

    # Insert
    europe = "EUROPE"
    brass = "BRASS"
    part_partsupp_probe = ps
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))
    
    part_partsupp_part = v0
    build_side = part_partsupp_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    v0 = part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_partkey] != None) else (None))
    
    region_nation_supplier_ps1_part_partsupp_probe = v0
    region_nation_supplier_ps1_probe = ps1
    region_nation_supplier_probe = su
    region_nation_probe = na
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    region_nation_part = v0
    build_side = region_nation_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_probe.sum(lambda x: ({build_side[x[0].n_regionkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].n_regionkey] != None) else (None))
    
    region_nation_supplier_part = v0
    build_side = region_nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    region_nation_supplier_ps1_part = v0
    build_side = region_nation_supplier_ps1_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_supplier_ps1_probe.sum(lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_suppkey] != None) else (None))
    
    v1 = v0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"ps_supplycost": x[0].ps_supplycost})})
    
    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})
    
    v3 = v2.sum(lambda x: {x[0].concat(record({"min_supplycost": x[0].ps_supplycost})): x[1]})
    
    region_nation_supplier_ps1_part_partsupp_part = v3
    build_side = region_nation_supplier_ps1_part_partsupp_part.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_supplier_ps1_part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_partkey] != None) else (None))
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe = v0
    region_nation_supplier_probe = su
    region_nation_probe = na
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))
    
    region_nation_part = v0
    build_side = region_nation_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_probe.sum(lambda x: ({build_side[x[0].n_regionkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].n_regionkey] != None) else (None))
    
    region_nation_supplier_part = v0
    build_side = region_nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    region_nation_supplier_ps1_part = v0
    build_side = region_nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    v1 = region_nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_part = v1
    build_side = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_suppkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_supplycost == x[0].min_supplycost) else (None))
    
    results = v1
    # Complete

    return results
