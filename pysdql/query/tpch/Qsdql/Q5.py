from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE, REGION_TYPE, NATION_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "re": REGION_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE})
def query(li, cu, ord, re, na, su):

    # Insert
    asia = "ASIA"
    region_nation_index = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == asia) else (None))
    
    region_nation_build_nest_dict = region_nation_index.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    region_nation_customer_index = na.sum(lambda x: (region_nation_build_nest_dict[x[0].n_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_build_nest_dict[x[0].n_regionkey] != None) else (None))
    
    region_nation_customer_build_nest_dict = region_nation_customer_index.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    region_nation_customer_orders_index = cu.sum(lambda x: (region_nation_customer_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_customer_build_nest_dict[x[0].c_nationkey] != None) else (None))
    
    region_nation_customer_orders_probe = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19940101) * (x[0].o_orderdate < 19950101))) else (None))
    
    region_nation_customer_orders_build_nest_dict = region_nation_customer_orders_index.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    region_nation_customer_orders_lineitem_index = region_nation_customer_orders_probe.sum(lambda x: (region_nation_customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))
    
    region_nation_customer_orders_lineitem_build_nest_dict = region_nation_customer_orders_lineitem_index.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    supplier_region_nation_customer_orders_lineitem_probe = li.sum(lambda x: (region_nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    supplier_region_nation_customer_orders_lineitem_build_nest_dict = su.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_nationkey": x[0].s_nationkey}): sr_dict({x[0]: x[1]})})
    
    supplier_region_nation_customer_orders_lineitem_0 = supplier_region_nation_customer_orders_lineitem_probe.sum(lambda x: (supplier_region_nation_customer_orders_lineitem_build_nest_dict[record({"l_suppkey": x[0].l_suppkey, "c_nationkey": x[0].c_nationkey})].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_region_nation_customer_orders_lineitem_build_nest_dict[record({"l_suppkey": x[0].l_suppkey, "c_nationkey": x[0].c_nationkey})] != None) else (None))
    
    supplier_region_nation_customer_orders_lineitem_1 = supplier_region_nation_customer_orders_lineitem_0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    supplier_region_nation_customer_orders_lineitem_2 = supplier_region_nation_customer_orders_lineitem_1.sum(lambda x: {record({"n_name": x[0].n_name}): record({"revenue": x[0].revenue})})
    
    results = supplier_region_nation_customer_orders_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
