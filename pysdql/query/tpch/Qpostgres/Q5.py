from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE, REGION_TYPE, NATION_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "re": REGION_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE})
def query(li, cu, ord, re, na, su):

    # Insert
    asia = "ASIA"
    nation_region_probe = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == asia) else (None))
    
    nation_region_build_nest_dict = na.sum(lambda x: {x[0].n_regionkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_region_probe = nation_region_probe.sum(lambda x: (nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    supplier_nation_region_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_region_probe = supplier_nation_region_probe.sum(lambda x: (supplier_nation_region_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_region_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_supplier_nation_region_build_nest_dict = li.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_region_orders_index = lineitem_supplier_nation_region_probe.sum(lambda x: (lineitem_supplier_nation_region_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_region_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    lineitem_supplier_nation_region_orders_probe = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19940101) * (x[0].o_orderdate < 19950101))) else (None))
    
    lineitem_supplier_nation_region_orders_build_nest_dict = lineitem_supplier_nation_region_orders_index.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_region_orders_customer_index = lineitem_supplier_nation_region_orders_probe.sum(lambda x: (lineitem_supplier_nation_region_orders_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_region_orders_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_supplier_nation_region_orders_customer_build_nest_dict = lineitem_supplier_nation_region_orders_customer_index.sum(lambda x: {record({"o_custkey": x[0].o_custkey, "s_nationkey": x[0].s_nationkey}): sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_region_orders_customer_0 = cu.sum(lambda x: (lineitem_supplier_nation_region_orders_customer_build_nest_dict[record({"c_custkey": x[0].c_custkey, "c_nationkey": x[0].c_nationkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_region_orders_customer_build_nest_dict[record({"c_custkey": x[0].c_custkey, "c_nationkey": x[0].c_nationkey})] != None) else (None))
    
    lineitem_supplier_nation_region_orders_customer_1 = lineitem_supplier_nation_region_orders_customer_0.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_supplier_nation_region_orders_customer_2 = lineitem_supplier_nation_region_orders_customer_1.sum(lambda x: {record({"n_name": x[0].n_name}): record({"revenue": x[0].before_1})})
    
    results = lineitem_supplier_nation_region_orders_customer_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
