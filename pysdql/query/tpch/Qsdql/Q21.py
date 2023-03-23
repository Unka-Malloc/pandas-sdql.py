from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "l2": LINEITEM_TYPE, "l3": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE})
def query(su, li, l2, l3, ord, na):

    # Insert
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))
    
    l2_lineitem_probe = v0
    v0 = l2.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"l2_size": 1.0})})
    
    v1 = v0.sum(lambda x: {x[0].concat(x[1]): True})
    
    l2_lineitem_part = v1
    build_side = l2_lineitem_part.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    v0 = l2_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    l3_l2_lineitem_probe = v0
    v0 = l3.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))
    
    v1 = v0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"l3_size": 1.0})})
    
    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})
    
    l3_l2_lineitem_part = v2
    build_side = l3_l2_lineitem_part.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    v0 = l3_l2_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    nation_supplier_l3_l2_lineitem_probe = v0
    nation_supplier_probe = su
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == saudiarabia) else (None))
    
    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    nation_supplier_l3_l2_lineitem_part = v0
    build_side = nation_supplier_l3_l2_lineitem_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    v0 = nation_supplier_l3_l2_lineitem_probe.sum(lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_suppkey] != None) else (None))
    
    orders_nation_supplier_l3_l2_lineitem_probe = v0
    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderstatus == f) else (None))
    
    orders_nation_supplier_l3_l2_lineitem_part = v0
    build_side = orders_nation_supplier_l3_l2_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    v0 = orders_nation_supplier_l3_l2_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l2_size > 1) * (x[0].l3_size == 1))) else (None))
    
    v2 = v1.sum(lambda x: {record({"s_name": x[0].s_name}): record({"numwait": 1.0})})
    
    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})
    
    results = v3
    # Complete

    return results
