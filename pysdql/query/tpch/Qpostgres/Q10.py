from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    r = "R"
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_returnflag == r) else (None))
    
    lineitem_orders_build_pre_ops = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    orders_0 = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19931001) * (x[0].o_orderdate < 19940101))) else (None))
    
    lineitem_orders_probe_pre_ops = orders_0.sum(lambda x: {record({"o_custkey": x[0].o_custkey, "o_orderkey": x[0].o_orderkey}): True})
    
    lineitem_orders_build_nest_dict = lineitem_orders_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_0 = lineitem_orders_probe_pre_ops.sum(lambda x: (lineitem_orders_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    customer_lineitem_orders_probe_pre_ops = lineitem_orders_0.sum(lambda x: {record({"o_custkey": x[0].o_custkey, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    customer_lineitem_orders_build_nest_dict = cu.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    customer_lineitem_orders_0 = customer_lineitem_orders_probe_pre_ops.sum(lambda x: (customer_lineitem_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_lineitem_orders_build_nest_dict[x[0].o_custkey] != None) else (None))
    
    customer_lineitem_orders_nation_build_pre_ops = customer_lineitem_orders_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment, "c_nationkey": x[0].c_nationkey, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    customer_lineitem_orders_nation_build_nest_dict = customer_lineitem_orders_nation_build_pre_ops.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    customer_lineitem_orders_nation_0 = na.sum(lambda x: (customer_lineitem_orders_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_lineitem_orders_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    customer_lineitem_orders_nation_1 = customer_lineitem_orders_nation_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "n_name": x[0].n_name, "c_name": x[0].c_name, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "c_acctbal": x[0].c_acctbal, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment}): True})
    
    customer_lineitem_orders_nation_1 = customer_lineitem_orders_nation_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "n_name": x[0].n_name, "c_name": x[0].c_name, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "c_acctbal": x[0].c_acctbal, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment}): True})
    
    customer_lineitem_orders_nation_1 = customer_lineitem_orders_nation_0.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    customer_lineitem_orders_nation_2 = customer_lineitem_orders_nation_1.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "n_name": x[0].n_name, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment}): record({"revenue": x[0].before_1})})
    
    customer_lineitem_orders_nation_3 = customer_lineitem_orders_nation_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    customer_lineitem_orders_nation_4 = customer_lineitem_orders_nation_3.sum(lambda x: {record({"c_name": x[0].c_name, "revenue": x[0].revenue, "c_acctbal": x[0].c_acctbal, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment}): True})
    
    results = customer_lineitem_orders_nation_4.sum(lambda x: {record({"c_name": x[0].c_name, "revenue": x[0].revenue, "c_acctbal": x[0].c_acctbal, "c_address": x[0].c_address, "c_phone": x[0].c_phone, "c_comment": x[0].c_comment}): True})
    
    # Complete

    return results
