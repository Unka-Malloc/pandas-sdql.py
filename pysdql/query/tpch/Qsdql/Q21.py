from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE})
def query(su, li, ord, na):

    # Insert
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    orders_nation_supplier_l3_l2_lineitem_build_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderstatus == f) else (None))
    
    nation_supplier_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == saudiarabia) else (None))
    
    nation_supplier_build_nest_dict = nation_supplier_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_l3_l2_lineitem_build_pre_ops = su.sum(lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    l3_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))
    
    l3_1 = l3_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"l3_size": (1.0) if (x[0].l_suppkey != None) else (0.0)})})
    
    l3_2 = l3_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    l3_l2_lineitem_build_pre_ops = l3_2.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l3_size": x[0].l3_size}): True})
    
    l2_0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"l2_size": (1.0) if (x[0].l_suppkey != None) else (0.0)})})
    
    l2_1 = l2_0.sum(lambda x: {x[0].concat(x[1]): True})
    
    l2_lineitem_build_pre_ops = l2_1.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l2_size": x[0].l2_size}): True})
    
    l2_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))
    
    l2_lineitem_build_nest_dict = l2_lineitem_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    l3_l2_lineitem_probe_pre_ops = l2_lineitem_probe_pre_ops.sum(lambda x: (l2_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (l2_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    l3_l2_lineitem_build_nest_dict = l3_l2_lineitem_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_l3_l2_lineitem_probe_pre_ops = l3_l2_lineitem_probe_pre_ops.sum(lambda x: (l3_l2_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (l3_l2_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    nation_supplier_l3_l2_lineitem_build_nest_dict = nation_supplier_l3_l2_lineitem_build_pre_ops.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    orders_nation_supplier_l3_l2_lineitem_probe_pre_ops = nation_supplier_l3_l2_lineitem_probe_pre_ops.sum(lambda x: (nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))
    
    orders_nation_supplier_l3_l2_lineitem_build_nest_dict = orders_nation_supplier_l3_l2_lineitem_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    orders_nation_supplier_l3_l2_lineitem_0 = orders_nation_supplier_l3_l2_lineitem_probe_pre_ops.sum(lambda x: (orders_nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    orders_nation_supplier_l3_l2_lineitem_1 = orders_nation_supplier_l3_l2_lineitem_0.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l2_size > 1) * (x[0].l3_size == 1))) else (None))
    
    orders_nation_supplier_l3_l2_lineitem_2 = orders_nation_supplier_l3_l2_lineitem_1.sum(lambda x: {record({"s_name": x[0].s_name}): record({"numwait": (1.0) if (x[0].s_name != None) else (0.0)})})
    
    results = orders_nation_supplier_l3_l2_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
