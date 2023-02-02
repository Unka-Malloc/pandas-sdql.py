from pysdql.query.tpch.const import (SUPPLIER_TYPE, NATION_TYPE, PARTSUPP_TYPE, PART_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "na": NATION_TYPE, "ps": PARTSUPP_TYPE, "pa": PART_TYPE, "li": LINEITEM_TYPE})
def query(su, na, ps, pa, li):

    # Insert
    canada = "CANADA"
    forest = "forest"
    nation_part = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: True}) if (x_nation[0].n_name == canada) else (None))
    
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: True}) if (startsWith(x_part[0].p_name, forest)) else (None))
    
    supplier_part = su.sum(lambda x_supplier: {x_supplier[0].s_suppkey: True})
    
    lineitem_part = li.sum(lambda x_lineitem: ((((({record({"l_partkey": x_lineitem[0].l_partkey, "l_suppkey": x_lineitem[0].l_suppkey}): record({"sum_quantity": x_lineitem[0].l_quantity})}) if (part_part[x_lineitem[0].l_partkey] != None) else (None)) if (supplier_part[x_lineitem[0].l_suppkey] != None) else (None)) if (part_part[x_lineitem[0].l_partkey] != None) else (None)) if (supplier_part[x_lineitem[0].l_suppkey] != None) else (None)) if (((x_lineitem[0].l_shipdate >= 19940101) * (x_lineitem[0].l_shipdate < 19950101))) else (None))
    
    lineitem_partsupp = ps.sum(lambda x_partsupp: (({x_partsupp[0].ps_suppkey: True}) if (x_partsupp[0].ps_availqty > ((lineitem_part[record({"ps_partkey": x_partsupp[0].ps_partkey, "ps_suppkey": x_partsupp[0].ps_suppkey})].sum_quantity) * (0.5))) else (None)) if (lineitem_part[record({"ps_partkey": x_partsupp[0].ps_partkey, "ps_suppkey": x_partsupp[0].ps_suppkey})] != None) else (None))
    
    results = su.sum(lambda x_supplier: (({record({"s_name": x_supplier[0].s_name, "s_address": x_supplier[0].s_address}): True}) if (nation_part[x_supplier[0].s_nationkey] != None) else (None)) if (lineitem_partsupp[x_supplier[0].s_suppkey] != None) else (None))
    
    # Complete

    return results
