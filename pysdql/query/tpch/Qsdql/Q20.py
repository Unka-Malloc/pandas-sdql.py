from pysdql.query.tpch.const import (SUPPLIER_TYPE, NATION_TYPE, PARTSUPP_TYPE, PART_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "na": NATION_TYPE, "ps": PARTSUPP_TYPE, "pa": PART_TYPE, "li": LINEITEM_TYPE})
def query(su, na, ps, pa, li):

    # Insert
    canada = "CANADA"
    forest = "forest"
    lineitem_supplier_isin_build = su
    lineitem_partsupp_probe = ps
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (startsWith(x[0].p_name, forest)) else (None))
    
    lineitem_part_isin_build = v0
    lineitem_supplier_isin_build = su
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) else (None))
    
    lineitem_part_isin_build = lineitem_part_isin_build.sum(lambda x: {x[0].p_partkey: True})
    
    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (lineitem_part_isin_build[x[0].l_partkey] != None) else (None))
    
    lineitem_supplier_isin_build = su.sum(lambda x: {x[0].s_suppkey: True})
    
    v2 = v1.sum(lambda x: ({x[0]: x[1]}) if (lineitem_supplier_isin_build[x[0].l_suppkey] != None) else (None))
    
    v3 = v2.sum(lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): record({"sum_quantity": x[0].l_quantity})})
    
    v4 = v3.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_partsupp_part = v4
    build_side = lineitem_partsupp_part.sum(lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): sr_dict({x[0]: x[1]})})
    
    v0 = lineitem_partsupp_probe.sum(lambda x: ({build_side[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_availqty > ((x[0].sum_quantity) * (0.5))) else (None))
    
    supplier_lineitem_partsupp_isin_build = v1
    supplier_lineitem_partsupp_isin_build = supplier_lineitem_partsupp_isin_build.sum(lambda x: {x[0].l_suppkey: True})
    
    v0 = su.sum(lambda x: ({x[0]: x[1]}) if (supplier_lineitem_partsupp_isin_build[x[0].s_suppkey] != None) else (None))
    
    nation_supplier_probe = v0
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == canada) else (None))
    
    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    results = v0
    # Complete

    return results
