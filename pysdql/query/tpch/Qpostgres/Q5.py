from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE, REGION_TYPE, NATION_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "re": REGION_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE})
def query(li, cu, ord, re, na, su):

    # Insert
    asia = "ASIA"
    lineitem_supplier_nation_region_orders_customer_probe = cu
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (((x[0].o_orderdate >= 19940101) * (x[0].o_orderdate < 19950101))) else (None)) if (x[0] != None) else (None))
    
    lineitem_supplier_nation_region_orders_probe = v0
    v0 = re.sum(lambda x: (({x[0]: x[1]}) if (x[0].r_name == asia) else (None)) if (x[0] != None) else (None))
    
    nation_region_probe = v0
    nation_region_part = na
    build_side = nation_region_part.sum(lambda x: (({x[0].n_regionkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_region_probe.sum(lambda x: (({build_side[x[0].r_regionkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].r_regionkey] != None) else (None)) if (x[0] != None) else (None))
    
    supplier_nation_region_probe = v0
    supplier_nation_region_part = su
    build_side = supplier_nation_region_part.sum(lambda x: (({x[0].s_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = supplier_nation_region_probe.sum(lambda x: (({build_side[x[0].n_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].n_nationkey] != None) else (None)) if (x[0] != None) else (None))
    
    lineitem_supplier_nation_region_probe = v0
    lineitem_supplier_nation_region_part = li
    build_side = lineitem_supplier_nation_region_part.sum(lambda x: (({x[0].l_suppkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = lineitem_supplier_nation_region_probe.sum(lambda x: (({build_side[x[0].s_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_suppkey] != None) else (None)) if (x[0] != None) else (None))
    
    lineitem_supplier_nation_region_orders_part = v0
    build_side = lineitem_supplier_nation_region_orders_part.sum(lambda x: (({x[0].l_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = lineitem_supplier_nation_region_orders_probe.sum(lambda x: (({build_side[x[0].o_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_orderkey] != None) else (None)) if (x[0] != None) else (None))
    
    lineitem_supplier_nation_region_orders_customer_part = v0
    build_side = lineitem_supplier_nation_region_orders_customer_part.sum(lambda x: (({record({"o_custkey": x[0].o_custkey, "s_nationkey": x[0].s_nationkey}): sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = lineitem_supplier_nation_region_orders_customer_probe.sum(lambda x: (({build_side[record({"c_custkey": x[0].c_custkey, "c_nationkey": x[0].c_nationkey})].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[record({"c_custkey": x[0].c_custkey, "c_nationkey": x[0].c_nationkey})] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0].concat(record({"before_1": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({record({"n_name": x[0].n_name}): record({"revenue": x[0].before_1})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v3
    # Complete

    return results
