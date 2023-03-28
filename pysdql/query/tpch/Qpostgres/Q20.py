from pysdql.query.tpch.const import (SUPPLIER_TYPE, NATION_TYPE, PARTSUPP_TYPE, PART_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "na": NATION_TYPE, "ps": PARTSUPP_TYPE, "pa": PART_TYPE, "li": LINEITEM_TYPE})
def query(su, na, ps, pa, li):

    # Insert
    canada = "CANADA"
    forest = "forest"
    supplier_nation_probe_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == canada) else (None))
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_0 = supplier_nation_probe_pre_ops.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_partsupp_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) else (None))
    
    lineitem_partsupp_build_nest_dict = lineitem_partsupp_build_pre_ops.sum(lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): sr_dict({x[0]: x[1]})})
    
    lineitem_partsupp_0 = ps.sum(lambda x: (lineitem_partsupp_build_nest_dict[record({"l_partkey": x[0].ps_partkey, "l_suppkey": x[0].ps_suppkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_partsupp_build_nest_dict[record({"l_partkey": x[0].ps_partkey, "l_suppkey": x[0].ps_suppkey})] != None) else (None))
    
    lineitem_partsupp_1 = lineitem_partsupp_0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey}): record({"sum_l_quantity": x[0].l_quantity})})
    
    lineitem_partsupp_2 = lineitem_partsupp_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    partsupp_lineitem_partsupp_probe_pre_ops = lineitem_partsupp_2.sum(lambda x: {x[0].concat(record({"suml_quantity": ((0.5) * (x[0].sum_l_quantity))})): x[1]})
    
    partsupp_lineitem_partsupp_build_nest_dict = ps.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey}): sr_dict({x[0]: x[1]})})
    
    partsupp_lineitem_partsupp_0 = partsupp_lineitem_partsupp_probe_pre_ops.sum(lambda x: (partsupp_lineitem_partsupp_build_nest_dict[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_lineitem_partsupp_build_nest_dict[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})] != None) else (None))
    
    partsupp_lineitem_partsupp_part_build_pre_ops = partsupp_lineitem_partsupp_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_availqty > x[0].suml_quantity) else (None))
    
    partsupp_lineitem_partsupp_part_probe_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (startsWith(x[0].p_name, forest)) else (None))
    
    partsupp_lineitem_partsupp_part_build_nest_dict = partsupp_lineitem_partsupp_part_build_pre_ops.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})
    
    partsupp_lineitem_partsupp_part_supplier_nation_isin_pre_ops = partsupp_lineitem_partsupp_part_probe_pre_ops.sum(lambda x: (partsupp_lineitem_partsupp_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_lineitem_partsupp_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    partsupp_lineitem_partsupp_part_supplier_nation_isin_build_index = partsupp_lineitem_partsupp_part_supplier_nation_isin_pre_ops.sum(lambda x: {x[0].ps_suppkey: True})
    
    supplier_nation_1 = supplier_nation_0.sum(lambda x: ({x[0]: x[1]}) if (partsupp_lineitem_partsupp_part_supplier_nation_isin_build_index[x[0].s_suppkey] != None) else (None))
    
    supplier_nation_2 = supplier_nation_1.sum(lambda x: {record({"s_name": x[0].s_name, "s_address": x[0].s_address}): True})
    
    results = supplier_nation_2.sum(lambda x: {record({"s_name": x[0].s_name, "s_address": x[0].s_address}): True})
    
    # Complete

    return results
