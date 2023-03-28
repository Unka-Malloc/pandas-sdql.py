from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):

    # Insert
    lineitem_0 = li.sum(lambda x: {record({"l_quantity": x[0].l_quantity, "l_orderkey": x[0].l_orderkey}): True})
    
    lineitem_0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    lineitem_0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"sum_l_quantity": x[0].l_quantity})})
    
    lineitem_1 = lineitem_0.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_2 = lineitem_1.sum(lambda x: {x[0].concat(record({"suml_quantity": x[0].sum_l_quantity})): x[1]})
    
    orders_lineitem_probe_pre_ops = lineitem_2.sum(lambda x: ({x[0]: x[1]}) if (x[0].suml_quantity > 300) else (None))
    
    orders_lineitem_build_nest_dict = ord.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_0 = orders_lineitem_probe_pre_ops.sum(lambda x: (orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    orders_lineitem_customer_build_pre_ops = orders_lineitem_0.sum(lambda x: {record({"o_orderkey": x[0].o_orderkey, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice, "o_custkey": x[0].o_custkey, "l_orderkey": x[0].l_orderkey}): True})
    
    orders_lineitem_customer_build_nest_dict = orders_lineitem_customer_build_pre_ops.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_customer_0 = cu.sum(lambda x: (orders_lineitem_customer_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_customer_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_orders_lineitem_customer_probe_pre_ops = orders_lineitem_customer_0.sum(lambda x: {record({"c_name": x[0].c_name, "c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice, "l_orderkey": x[0].l_orderkey}): True})
    
    lineitem_orders_lineitem_customer_build_nest_dict = li.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_lineitem_customer_0 = lineitem_orders_lineitem_customer_probe_pre_ops.sum(lambda x: (lineitem_orders_lineitem_customer_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_lineitem_customer_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_orders_lineitem_customer_1 = lineitem_orders_lineitem_customer_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey, "c_name": x[0].c_name, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice, "l_quantity": x[0].l_quantity}): True})
    
    lineitem_orders_lineitem_customer_1 = lineitem_orders_lineitem_customer_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey, "c_name": x[0].c_name, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice}): record({"suml_quantity": x[0].l_quantity})})
    
    lineitem_orders_lineitem_customer_2 = lineitem_orders_lineitem_customer_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_lineitem_customer_3 = lineitem_orders_lineitem_customer_2.sum(lambda x: {record({"c_name": x[0].c_name, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice, "suml_quantity": x[0].suml_quantity}): True})
    
    results = lineitem_orders_lineitem_customer_3.sum(lambda x: {record({"c_name": x[0].c_name, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice, "suml_quantity": x[0].suml_quantity}): True})
    
    # Complete

    return results
