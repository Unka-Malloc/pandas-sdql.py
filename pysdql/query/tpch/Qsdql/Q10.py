from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    r = "R"
    v0 = li.sum(lambda x: (({x[0]: x[1]}) if (x[0].l_returnflag == r) else (None)) if (x[0] != None) else (None))
    
    nation_customer_orders_lineitem_probe = v0
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (((x[0].o_orderdate >= 19931001) * (x[0].o_orderdate < 19940101))) else (None)) if (x[0] != None) else (None))
    
    customer_orders_probe = v0
    v0 = cu.sum(lambda x: (({x[0]: x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    customer_orders_part = v0
    build_side = customer_orders_part.sum(lambda x: (({x[0].c_custkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = customer_orders_probe.sum(lambda x: (({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_custkey] != None) else (None)) if (x[0] != None) else (None))
    
    nation_customer_orders_probe = v0
    nation_customer_orders_part = na
    build_side = nation_customer_orders_part.sum(lambda x: (({x[0].n_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_customer_orders_probe.sum(lambda x: (({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].c_nationkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    nation_customer_orders_lineitem_part = v1
    build_side = nation_customer_orders_lineitem_part.sum(lambda x: (({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_customer_orders_lineitem_probe.sum(lambda x: (({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({record({"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_phone": x[0].c_phone, "n_name": x[0].n_name, "c_address": x[0].c_address, "c_comment": x[0].c_comment}): record({"revenue": x[0].revenue})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v3
    # Complete

    return results