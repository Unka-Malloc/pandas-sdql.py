from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    r = "R"
    customer_orders_probe_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19931001) * (x[0].o_orderdate < 19940101))) else (None))
    
    customer_orders_build_nest_dict = cu.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    nation_customer_orders_probe_pre_ops = customer_orders_probe_pre_ops.sum(lambda x: (customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))
    
    nation_customer_orders_build_nest_dict = na.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    nation_customer_orders_lineitem_build_pre_ops = nation_customer_orders_probe_pre_ops.sum(lambda x: (nation_customer_orders_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_customer_orders_build_nest_dict[x[0].c_nationkey] != None) else (None))
    
    nation_customer_orders_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_returnflag == r) else (None))
    
    nation_customer_orders_lineitem_build_nest_dict = nation_customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    nation_customer_orders_lineitem_0 = nation_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    nation_customer_orders_lineitem_1 = nation_customer_orders_lineitem_0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    nation_customer_orders_lineitem_2 = nation_customer_orders_lineitem_1.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_phone": x[0].c_phone, "n_name": x[0].n_name, "c_address": x[0].c_address, "c_comment": x[0].c_comment}): record({"revenue": x[0].revenue})})
    
    results = nation_customer_orders_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results