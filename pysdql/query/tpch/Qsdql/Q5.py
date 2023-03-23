from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE, REGION_TYPE, NATION_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "re": REGION_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE})
def query(li, cu, ord, re, na, su):

    # Insert
    asia = "ASIA"
    region_nation_customer_orders_lineitem_probe = li
    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19940101) * (x[0].o_orderdate < 19961231))) else (None))
    
    region_nation_customer_orders_probe = v0
    region_nation_customer_probe = cu
    region_nation_probe = na
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == asia) else (None))
    
    region_nation_part = v0
    build_side = region_nation_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_probe.sum(lambda x: ({build_side[x[0].n_regionkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].n_regionkey] != None) else (None))
    
    region_nation_customer_part = v0
    build_side = region_nation_customer_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_customer_probe.sum(lambda x: ({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].c_nationkey] != None) else (None))
    
    region_nation_customer_orders_part = v0
    build_side = region_nation_customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_custkey] != None) else (None))
    
    region_nation_customer_orders_lineitem_part = v0
    build_side = region_nation_customer_orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    v0 = region_nation_customer_orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    supplier_region_nation_customer_orders_lineitem_probe = v0
    supplier_region_nation_customer_orders_lineitem_part = su
    build_side = supplier_region_nation_customer_orders_lineitem_part.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_nationkey": x[0].s_nationkey}): sr_dict({x[0]: x[1]})})
    
    v0 = supplier_region_nation_customer_orders_lineitem_probe.sum(lambda x: ({build_side[record({"l_suppkey": x[0].l_suppkey, "c_nationkey": x[0].c_nationkey})].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[record({"l_suppkey": x[0].l_suppkey, "c_nationkey": x[0].c_nationkey})] != None) else (None))
    
    v1 = v0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    v2 = v1.sum(lambda x: {record({"n_name": x[0].n_name}): record({"revenue": x[0].revenue})})
    
    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})
    
    results = v3
    # Complete

    return results
