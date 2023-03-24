from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, na, re):

    # Insert
    europe = "EUROPE"
    brass = "BRASS"
    region_part = re.sum(lambda x_region: ({x_region[0].r_regionkey: True}) if (x_region[0].r_name == europe) else (None))
    
    region_nation = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: record({"n_nationkey": x_nation[0].n_nationkey, "n_name": x_nation[0].n_name})}) if (region_part[x_nation[0].n_regionkey] != None) else (None))
    
    region_nation_supplier = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: record({"s_suppkey": x_supplier[0].s_suppkey, "s_acctbal": x_supplier[0].s_acctbal, "s_name": x_supplier[0].s_name, "n_name": region_nation[x_supplier[0].s_nationkey].n_name, "s_address": x_supplier[0].s_address, "s_phone": x_supplier[0].s_phone, "s_comment": x_supplier[0].s_comment})}) if (region_nation[x_supplier[0].s_nationkey] != None) else (None))
    
    region_nation_supplier_partsupp = ps.sum(lambda x_partsupp: ({x_partsupp[0].ps_partkey: record({"min_supplycost": x_partsupp[0].ps_supplycost, "ps_partkey": x_partsupp[0].ps_partkey, "ps_suppkey": x_partsupp[0].ps_suppkey, "s_suppkey": x_partsupp[0].ps_suppkey})}) if (region_nation_supplier[x_partsupp[0].ps_suppkey] != None) else (None))
    
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: record({"p_partkey": x_part[0].p_partkey, "p_mfgr": x_part[0].p_mfgr})}) if (((endsWith(x_part[0].p_type, brass)) * (x_part[0].p_size == 15))) else (None))
    
    results = ps.sum(lambda x_partsupp: (((({record({"s_acctbal": region_nation_supplier[x_partsupp[0].ps_suppkey].s_acctbal, "s_name": region_nation_supplier[x_partsupp[0].ps_suppkey].s_name, "n_name": region_nation_supplier[x_partsupp[0].ps_suppkey].n_name, "p_partkey": part_part[x_partsupp[0].ps_partkey].p_partkey, "p_mfgr": part_part[x_partsupp[0].ps_partkey].p_mfgr, "s_address": region_nation_supplier[x_partsupp[0].ps_suppkey].s_address, "s_phone": region_nation_supplier[x_partsupp[0].ps_suppkey].s_phone, "s_comment": region_nation_supplier[x_partsupp[0].ps_suppkey].s_comment}): True}) if (x_partsupp[0].ps_supplycost == region_nation_supplier_partsupp[x_partsupp[0].ps_partkey].min_supplycost) else (None)) if (region_nation_supplier[x_partsupp[0].ps_suppkey] != None) else (None)) if (region_nation_supplier_partsupp[x_partsupp[0].ps_partkey] != None) else (None)) if (part_part[x_partsupp[0].ps_partkey] != None) else (None))
    
    # Complete

    return results
