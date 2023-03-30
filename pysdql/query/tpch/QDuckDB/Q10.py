from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    lineitem_orders_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_returnflag == r) else (None))
    
    lineitem_orders_probe_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19931001) * (x[0].o_orderdate < 19940101))) else (None))
    
    lineitem_orders_build_nest_dict = lineitem_orders_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_build_pre_ops = lineitem_orders_probe_pre_ops.sum(lambda x: (lineitem_orders_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    customer_nation_build_nest_dict = cu.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_probe_pre_ops = na.sum(lambda x: (customer_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_orders_customer_nation_build_nest_dict = lineitem_orders_customer_nation_build_pre_ops.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_0 = lineitem_orders_customer_nation_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_nation_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_nation_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_orders_customer_nation_1 = lineitem_orders_customer_nation_0.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_orders_customer_nation_2 = lineitem_orders_customer_nation_1.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_phone": x[0].c_phone, "n_name": x[0].n_name, "c_address": x[0].c_address, "c_comment": x[0].c_comment}): record({"revenue": x[0].before_1})})
    
    lineitem_orders_customer_nation_3 = lineitem_orders_customer_nation_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_customer_nation_attach_to_df_aggr_1 = lineitem_orders_customer_nation_3.sum(lambda x: {x[0]: x[1]})
    
    results = lineitem_orders_customer_nation_attach_to_df_aggr_1.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "revenue": x[0].revenue, "c_acctbal": x[0].c_acctbal, "n_name": x[0].n_name, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment}): True})
    
    # Complete

    return results
