from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "l2": LINEITEM_TYPE, "l3": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE})
def query(su, li, l2, l3, ord, na):

    # Insert
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    lineitem_orders_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))
    
    lineitem_orders_probe_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderstatus == f) else (None))
    
    lineitem_orders_build_nest_dict = lineitem_orders_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_supplier_nation_build_pre_ops = lineitem_orders_probe_pre_ops.sum(lambda x: (lineitem_orders_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    supplier_nation_probe_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == saudiarabia) else (None))
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_supplier_nation_probe_pre_ops = supplier_nation_probe_pre_ops.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_orders_supplier_nation_build_nest_dict = lineitem_orders_supplier_nation_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_supplier_nation_0 = lineitem_orders_supplier_nation_probe_pre_ops.sum(lambda x: (lineitem_orders_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    lineitem_orders_supplier_nation_1 = lineitem_orders_supplier_nation_0.sum(lambda x: {x[0]: sr_dict({record({"l_suppkey_x": x[0].l_suppkey, "l_suppkey": x[0].l_suppkey, "l_orderkey": x[0].l_orderkey, "l_receiptdate": x[0].l_receiptdate, "l_commitdate": x[0].l_commitdate, "o_orderkey": x[0].o_orderkey, "o_orderstatus": x[0].o_orderstatus, "s_suppkey": x[0].s_suppkey, "s_nationkey": x[0].s_nationkey, "s_name": x[0].s_name, "n_nationkey": x[0].n_nationkey, "n_name": x[0].n_name}): True})})
    
    lineitem_orders_supplier_nation_lineitem_build_pre_ops = lineitem_orders_supplier_nation_1.sum(lambda x: x[1])
    
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {x[0]: sr_dict({record({"l_orderkey_y": x[0].l_orderkey, "l_orderkey": x[0].l_orderkey, "l_suppkey_y": x[0].l_suppkey, "l_suppkey": x[0].l_suppkey, "l_receiptdate": x[0].l_receiptdate, "l_commitdate": x[0].l_commitdate}): True})})
    
    lineitem_orders_supplier_nation_lineitem_probe_pre_ops = lineitem_1.sum(lambda x: x[1])
    
    lineitem_orders_supplier_nation_lineitem_build_nest_dict = lineitem_orders_supplier_nation_lineitem_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_supplier_nation_lineitem_0 = lineitem_orders_supplier_nation_lineitem_probe_pre_ops.sum(lambda x: (lineitem_orders_supplier_nation_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_supplier_nation_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    lineitem_orders_supplier_nation_lineitem_1 = lineitem_orders_supplier_nation_lineitem_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_suppkey_x != x[0].l_suppkey_y) else (None))
    
    lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_build_pre_ops = lineitem_orders_supplier_nation_lineitem_1.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): True})
    
    lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_isin_pre_ops = lineitem_orders_supplier_nation_lineitem_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_suppkey_x != x[0].l_suppkey_y) else (None))
    
    lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_isin_build_index = lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_isin_pre_ops.sum(lambda x: {x[0].l_orderkey: True})
    
    lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_probe_pre_ops = lineitem_orders_supplier_nation_0.sum(lambda x: ({x[0]: x[1]}) if (lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_isin_build_index[x[0].l_orderkey] != None) else (None))
    
    lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_build_nest_dict = lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_0 = lineitem_orders_supplier_nation_lineitem_lineitem_orders_supplier_nation_probe_pre_ops.sum(lambda x: ({x[0]: True}) if (lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_build_nest_dict[x[0].l_orderkey] == None) else (None))
    
    lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_1 = lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_0.sum(lambda x: {record({"s_name": x[0].s_name}): record({"numwait": (1.0) if (x[0].s_name != None) else (0.0)})})
    
    lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_2 = lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_3 = lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_2.sum(lambda x: {x[0]: {record({"numwait": x[0].numwait}): True}})
    
    results = lineitem_orders_supplier_nation_lineitem_orders_supplier_nation_lineitem_3.sum(lambda x: x[1])
    
    # Complete

    return results
