from pysdql.query.tpch.const import (SUPPLIER_TYPE, NATION_TYPE, PARTSUPP_TYPE, PART_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "na": NATION_TYPE, "ps": PARTSUPP_TYPE, "pa": PART_TYPE, "li": LINEITEM_TYPE})
def query(su, na, ps, pa, li):

    # Insert
    canada = "CANADA"
    forest = "forest"
    nation_supplier_index = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == canada) else (None))
    
    part_0 = pa.sum(lambda x: ({x[0]: x[1]}) if (startsWith(x[0].p_name, forest)) else (None))
    
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) else (None))
    
    part_lineitem_isin_build_index = part_0.sum(lambda x: {x[0].p_partkey: True})
    
    lineitem_1 = lineitem_0.sum(lambda x: ({x[0]: x[1]}) if (part_lineitem_isin_build_index[x[0].l_partkey] != None) else (None))
    
    supplier_lineitem_isin_build_index = su.sum(lambda x: {x[0].s_suppkey: True})
    
    lineitem_2 = lineitem_1.sum(lambda x: ({x[0]: x[1]}) if (supplier_lineitem_isin_build_index[x[0].l_suppkey] != None) else (None))
    
    lineitem_3 = lineitem_2.sum(lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): record({"sum_quantity": x[0].l_quantity})})
    
    lineitem_partsupp_index = lineitem_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_partsupp_build_nest_dict = lineitem_partsupp_index.sum(lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): sr_dict({x[0]: x[1]})})
    
    lineitem_partsupp_0 = ps.sum(lambda x: (lineitem_partsupp_build_nest_dict[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_partsupp_build_nest_dict[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})] != None) else (None))
    
    lineitem_partsupp_1 = lineitem_partsupp_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_availqty > ((x[0].sum_quantity) * (0.5))) else (None))
    
    lineitem_partsupp_supplier_isin_build_index = lineitem_partsupp_1.sum(lambda x: {x[0].l_suppkey: True})
    
    nation_supplier_probe = su.sum(lambda x: ({x[0]: x[1]}) if (lineitem_partsupp_supplier_isin_build_index[x[0].s_suppkey] != None) else (None))
    
    nation_supplier_build_nest_dict = nation_supplier_index.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_0 = nation_supplier_probe.sum(lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    results = nation_supplier_0.sum(lambda x: {record({"s_name": x[0].s_name, "s_address": x[0].s_address}): True})
    
    # Complete

    return su
