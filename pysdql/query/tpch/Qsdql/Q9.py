from pysdql.query.tpch.const import (LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE, SUPPLIER_TYPE, PART_TYPE,
                                     PARTSUPP_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE, "pa": PART_TYPE,
               "ps": PARTSUPP_TYPE})
def query(li, ord, na, su, pa, ps):

    # Insert
    g = "g"
    nation_supplier_build_nest_dict = na.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_part_partsupp_index = su.sum(lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    part_partsupp_index = pa.sum(lambda x: ({x[0]: x[1]}) if (firstIndex(x[0].p_name, g) != ((-1) * (1))) else (None))
    
    part_partsupp_build_nest_dict = part_partsupp_index.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_part_partsupp_probe = ps.sum(lambda x: (part_partsupp_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))
    
    nation_supplier_part_partsupp_build_nest_dict = nation_supplier_part_partsupp_index.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_part_partsupp_orders_lineitem_index = nation_supplier_part_partsupp_probe.sum(lambda x: (nation_supplier_part_partsupp_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_part_partsupp_build_nest_dict[x[0].ps_suppkey] != None) else (None))
    
    orders_lineitem_build_nest_dict = ord.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_part_partsupp_orders_lineitem_probe = li.sum(lambda x: (orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    nation_supplier_part_partsupp_orders_lineitem_build_nest_dict = nation_supplier_part_partsupp_orders_lineitem_index.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey}): sr_dict({x[0]: x[1]})})
    
    nation_supplier_part_partsupp_orders_lineitem_0 = nation_supplier_part_partsupp_orders_lineitem_probe.sum(lambda x: (nation_supplier_part_partsupp_orders_lineitem_build_nest_dict[record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_part_partsupp_orders_lineitem_build_nest_dict[record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey})] != None) else (None))
    
    nation_supplier_part_partsupp_orders_lineitem_1 = nation_supplier_part_partsupp_orders_lineitem_0.sum(lambda x: {x[0].concat(record({"nation": x[0].n_name})): x[1]})
    
    nation_supplier_part_partsupp_orders_lineitem_2 = nation_supplier_part_partsupp_orders_lineitem_1.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})
    
    nation_supplier_part_partsupp_orders_lineitem_3 = nation_supplier_part_partsupp_orders_lineitem_2.sum(lambda x: {x[0].concat(record({"amount": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) - (((x[0].ps_supplycost) * (x[0].l_quantity))))})): x[1]})
    
    nation_supplier_part_partsupp_orders_lineitem_4 = nation_supplier_part_partsupp_orders_lineitem_3.sum(lambda x: {record({"nation": x[0].nation, "o_year": x[0].o_year, "amount": x[0].amount}): True})
    
    nation_supplier_part_partsupp_orders_lineitem_5 = nation_supplier_part_partsupp_orders_lineitem_4.sum(lambda x: {record({"nation": x[0].nation, "o_year": x[0].o_year}): record({"sum_profit": x[0].amount})})
    
    results = nation_supplier_part_partsupp_orders_lineitem_5.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
