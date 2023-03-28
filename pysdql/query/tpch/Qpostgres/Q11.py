from pysdql.query.tpch.const import (PARTSUPP_TYPE, SUPPLIER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ps": PARTSUPP_TYPE, "su": SUPPLIER_TYPE, "na": NATION_TYPE})
def query(ps, su, na):

    # Insert
    germany = "GERMANY"
    partsupp_0 = ps.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey, "ps_availqty": x[0].ps_availqty, "ps_supplycost": x[0].ps_supplycost, "ps_comment": x[0].ps_comment}): True})
    
    partsupp_supplier_nation_build_pre_ops = partsupp_0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey, "ps_availqty": x[0].ps_availqty, "ps_supplycost": x[0].ps_supplycost, "ps_comment": x[0].ps_comment}): True})
    
    supplier_0 = su.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_name": x[0].s_name, "s_address": x[0].s_address, "s_nationkey": x[0].s_nationkey, "s_phone": x[0].s_phone, "s_acctbal": x[0].s_acctbal, "s_comment": x[0].s_comment}): True})
    
    supplier_nation_build_pre_ops = supplier_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_name": x[0].s_name, "s_address": x[0].s_address, "s_nationkey": x[0].s_nationkey, "s_phone": x[0].s_phone, "s_acctbal": x[0].s_acctbal, "s_comment": x[0].s_comment}): True})
    
    nation_0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))
    
    supplier_nation_probe_pre_ops = nation_0.sum(lambda x: {record({"n_nationkey": x[0].n_nationkey}): True})
    
    supplier_nation_build_nest_dict = supplier_nation_build_pre_ops.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_0 = supplier_nation_probe_pre_ops.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    partsupp_supplier_nation_probe_pre_ops = supplier_nation_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey}): True})
    
    partsupp_supplier_nation_build_nest_dict = partsupp_supplier_nation_build_pre_ops.sum(lambda x: {x[0].ps_suppkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_0 = partsupp_supplier_nation_probe_pre_ops.sum(lambda x: (partsupp_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    partsupp_supplier_nation_1 = partsupp_supplier_nation_0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_supplycost": x[0].ps_supplycost, "ps_availqty": x[0].ps_availqty}): True})
    
    partsupp_supplier_nation_2 = partsupp_supplier_nation_1.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_supplycost": x[0].ps_supplycost, "ps_availqty": x[0].ps_availqty}): True})
    
    partsupp_supplier_nation_3 = partsupp_supplier_nation_2.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].ps_supplycost) * (x[0].ps_availqty))})): x[1]})
    
    partsupp_supplier_nation_4 = partsupp_supplier_nation_3.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"value": x[0].before_1, "sum_before_1": x[0].before_1})})
    
    partsupp_supplier_nation_5 = partsupp_supplier_nation_4.sum(lambda x: {x[0].concat(x[1]): True})
    
    partsupp_supplier_nation_6 = partsupp_supplier_nation_5.sum(lambda x: {x[0].concat(record({"sumps_supplycostps_availqty": x[0].sum_before_1})): x[1]})
    
    nation_0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))
    
    supplier_nation_probe_pre_ops = nation_0.sum(lambda x: {record({"n_nationkey": x[0].n_nationkey}): True})
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_0 = supplier_nation_probe_pre_ops.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    partsupp_supplier_nation_probe_pre_ops = supplier_nation_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey}): True})
    
    partsupp_supplier_nation_build_nest_dict = ps.sum(lambda x: {x[0].ps_suppkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_0 = partsupp_supplier_nation_probe_pre_ops.sum(lambda x: (partsupp_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    partsupp_supplier_nation_1 = partsupp_supplier_nation_0.sum(lambda x: {record({"ps_supplycost": x[0].ps_supplycost, "ps_availqty": x[0].ps_availqty}): True})
    
    partsupp_supplier_nation_2 = partsupp_supplier_nation_1.sum(lambda x: record({"JQ_ps_supplycost_mul_ps_availqty_XZ": ((x[0].ps_supplycost) * (x[0].ps_availqty))}))
    
    sumps_supplycostps_availqty00001_el_0_JQ_JQ_ps_supplycost_mul_ps_availqty_XZ_mul_00001_XZ = ((partsupp_supplier_nation_2.JQ_ps_supplycost_mul_ps_availqty_XZ) * (0.0001))
    partsupp_supplier_nation_7 = partsupp_supplier_nation_6.sum(lambda x: ({x[0]: x[1]}) if (x[0].sumps_supplycostps_availqty > sumps_supplycostps_availqty00001_el_0_JQ_JQ_ps_supplycost_mul_ps_availqty_XZ_mul_00001_XZ) else (None))
    
    partsupp_supplier_nation_8 = partsupp_supplier_nation_7.sum(lambda x: {record({"value": x[0].value}): True})
    
    results = partsupp_supplier_nation_8.sum(lambda x: {record({"value": x[0].value}): True})
    
    # Complete

    return results
