from pysdql.query.tpch.const import (LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE, SUPPLIER_TYPE, PART_TYPE,
                                     PARTSUPP_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE, "pa": PART_TYPE,
               "ps": PARTSUPP_TYPE})
def query(li, ord, na, su, pa, ps):

    # Insert
    g = "g"
    orders_lineitem_probe = li
    orders_lineitem_part = ord
    build_side = orders_lineitem_part.sum(lambda x: ({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    nation_supplier_part_partsupp_orders_lineitem_probe = v0
    part_partsupp_probe = ps
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (firstIndex(x[0].p_name, g) != ((-1) * (1))) else (None))
    
    part_partsupp_part = v0
    build_side = part_partsupp_part.sum(lambda x: ({x[0].p_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_partkey] != None) else (None))
    
    nation_supplier_part_partsupp_probe = v0
    nation_supplier_probe = su
    nation_supplier_part = na
    build_side = nation_supplier_part.sum(lambda x: ({x[0].n_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    nation_supplier_part_partsupp_part = v0
    build_side = nation_supplier_part_partsupp_part.sum(lambda x: ({x[0].s_suppkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = nation_supplier_part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_suppkey] != None) else (None))
    
    nation_supplier_part_partsupp_orders_lineitem_part = v0
    build_side = nation_supplier_part_partsupp_orders_lineitem_part.sum(lambda x: ({record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey}): sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = nation_supplier_part_partsupp_orders_lineitem_probe.sum(lambda x: ({build_side[record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey})].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey})] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"nation": x[0].n_name})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: ({x[0].concat(record({"amount": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) - (((x[0].ps_supplycost) * (x[0].l_quantity))))})): x[1]}) if (True) else (None))
    
    v4 = v3.sum(lambda x: ({record({"nation": x[0].nation, "o_year": x[0].o_year}): record({"sum_profit": x[0].amount})}) if (True) else (None))
    
    v5 = v4.sum(lambda x: ({x[0].concat(x[1]): True}) if (True) else (None))
    
    results = v5
    # Complete

    return results
