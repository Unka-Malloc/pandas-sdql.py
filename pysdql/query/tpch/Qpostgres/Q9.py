from pysdql.query.tpch.const import (LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE, SUPPLIER_TYPE, PART_TYPE,
                                     PARTSUPP_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE, "pa": PART_TYPE,
               "ps": PARTSUPP_TYPE})
def query(li, ord, na, su, pa, ps):

    # Insert
    green = "green"
    part_0 = pa.sum(lambda x: ({x[0]: x[1]}) if (firstIndex(x[0].p_name, green) != ((-1) * (1))) else (None))
    
    part_1 = part_0.sum(lambda x: {x[0]: {record({"p_partkey": x[0].p_partkey}): True}})
    
    lineitem_part_probe_pre_ops = part_1.sum(lambda x: x[1])
    
    lineitem_part_build_nest_dict = li.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_probe_pre_ops = lineitem_part_probe_pre_ops.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    orders_lineitem_part_build_nest_dict = ord.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_partsupp_build_pre_ops = orders_lineitem_part_probe_pre_ops.sum(lambda x: (orders_lineitem_part_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    orders_lineitem_part_partsupp_build_nest_dict = orders_lineitem_part_partsupp_build_pre_ops.sum(lambda x: {record({"l_suppkey": x[0].l_suppkey, "l_partkey": x[0].l_partkey}): sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_partsupp_supplier_build_pre_ops = ps.sum(lambda x: (orders_lineitem_part_partsupp_build_nest_dict[record({"l_suppkey": x[0].ps_suppkey, "l_partkey": x[0].ps_partkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_partsupp_build_nest_dict[record({"l_suppkey": x[0].ps_suppkey, "l_partkey": x[0].ps_partkey})] != None) else (None))
    
    orders_lineitem_part_partsupp_supplier_build_nest_dict = orders_lineitem_part_partsupp_supplier_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_partsupp_supplier_nation_build_pre_ops = su.sum(lambda x: (orders_lineitem_part_partsupp_supplier_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_partsupp_supplier_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    orders_lineitem_part_partsupp_supplier_nation_build_nest_dict = orders_lineitem_part_partsupp_supplier_nation_build_pre_ops.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_partsupp_supplier_nation_0 = na.sum(lambda x: (orders_lineitem_part_partsupp_supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_partsupp_supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_lineitem_part_partsupp_supplier_nation_1 = orders_lineitem_part_partsupp_supplier_nation_0.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})
    
    orders_lineitem_part_partsupp_supplier_nation_2 = orders_lineitem_part_partsupp_supplier_nation_1.sum(lambda x: {x[0].concat(record({"nation": x[0].n_name})): x[1]})
    
    orders_lineitem_part_partsupp_supplier_nation_3 = orders_lineitem_part_partsupp_supplier_nation_2.sum(lambda x: {x[0].concat(record({"amount": ((((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))) - (((x[0].ps_supplycost) * (x[0].l_quantity))))})): x[1]})
    
    orders_lineitem_part_partsupp_supplier_nation_4 = orders_lineitem_part_partsupp_supplier_nation_3.sum(lambda x: {record({"nation": x[0].nation, "o_year": x[0].o_year}): record({"sum_profit": x[0].amount})})
    
    orders_lineitem_part_partsupp_supplier_nation_5 = orders_lineitem_part_partsupp_supplier_nation_4.sum(lambda x: {x[0].concat(x[1]): True})
    
    orders_lineitem_part_partsupp_supplier_nation_6 = orders_lineitem_part_partsupp_supplier_nation_5.sum(lambda x: {x[0]: {record({"sum_profit": x[0].sum_profit}): True}})
    
    results = orders_lineitem_part_partsupp_supplier_nation_6.sum(lambda x: x[1])
    
    # Complete

    return results
