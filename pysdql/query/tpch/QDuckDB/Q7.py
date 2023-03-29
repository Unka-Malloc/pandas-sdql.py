from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    lineitem_orders_customer_nation_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None))
    
    customer_nation_build_nest_dict = cu.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_nation_probe_pre_ops = na.sum(lambda x: (customer_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_customer_nation_build_nest_dict = ord.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_probe_pre_ops = orders_customer_nation_probe_pre_ops.sum(lambda x: (orders_customer_nation_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_customer_nation_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_orders_customer_nation_build_nest_dict = lineitem_orders_customer_nation_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_0 = lineitem_orders_customer_nation_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_nation_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_nation_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_orders_customer_nation_1 = lineitem_orders_customer_nation_0.sum(lambda x: {x[0]: sr_dict({record({"l_suppkey": x[0].l_suppkey, "l_orderkey": x[0].l_orderkey, "l_shipdate": x[0].l_shipdate, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "o_orderkey": x[0].o_orderkey, "o_custkey": x[0].o_custkey, "o_orderstatus": x[0].o_orderstatus, "o_totalprice": x[0].o_totalprice, "o_orderdate": x[0].o_orderdate, "o_orderpriority": x[0].o_orderpriority, "o_clerk": x[0].o_clerk, "o_shippriority": x[0].o_shippriority, "o_comment": x[0].o_comment, "c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_address": x[0].c_address, "c_nationkey": x[0].c_nationkey, "c_phone": x[0].c_phone, "c_acctbal": x[0].c_acctbal, "c_mktsegment": x[0].c_mktsegment, "c_comment": x[0].c_comment, "n_nationkey_x": x[0].n_nationkey, "n_name_x": x[0].n_name, "n_regionkey": x[0].n_regionkey, "n_comment": x[0].n_comment}): True})})
    
    lineitem_orders_customer_nation_supplier_nation_build_pre_ops = lineitem_orders_customer_nation_1.sum(lambda x: x[1])
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_0 = na.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    supplier_nation_1 = supplier_nation_0.sum(lambda x: {x[0]: sr_dict({record({"s_suppkey": x[0].s_suppkey, "s_name": x[0].s_name, "s_address": x[0].s_address, "s_nationkey": x[0].s_nationkey, "s_phone": x[0].s_phone, "s_acctbal": x[0].s_acctbal, "s_comment": x[0].s_comment, "n_nationkey_y": x[0].n_nationkey, "n_name_y": x[0].n_name}): True})})
    
    lineitem_orders_customer_nation_supplier_nation_probe_pre_ops = supplier_nation_1.sum(lambda x: x[1])
    
    lineitem_orders_customer_nation_supplier_nation_build_nest_dict = lineitem_orders_customer_nation_supplier_nation_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_supplier_nation_0 = lineitem_orders_customer_nation_supplier_nation_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_nation_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_nation_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    lineitem_orders_customer_nation_supplier_nation_1 = lineitem_orders_customer_nation_supplier_nation_0.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].n_name_x == france) * (x[0].n_name_y == germany))) + (((x[0].n_name_x == germany) * (x[0].n_name_y == france))))) else (None))
    
    lineitem_orders_customer_nation_supplier_nation_2 = lineitem_orders_customer_nation_supplier_nation_1.sum(lambda x: {x[0].concat(record({"n1n_name": x[0].n_name_y})): x[1]})
    
    lineitem_orders_customer_nation_supplier_nation_3 = lineitem_orders_customer_nation_supplier_nation_2.sum(lambda x: {x[0].concat(record({"n2n_name": x[0].n_name_x})): x[1]})
    
    lineitem_orders_customer_nation_supplier_nation_4 = lineitem_orders_customer_nation_supplier_nation_3.sum(lambda x: {x[0].concat(record({"supp_nation": x[0].n1n_name})): x[1]})
    
    lineitem_orders_customer_nation_supplier_nation_5 = lineitem_orders_customer_nation_supplier_nation_4.sum(lambda x: {x[0].concat(record({"cust_nation": x[0].n2n_name})): x[1]})
    
    lineitem_orders_customer_nation_supplier_nation_6 = lineitem_orders_customer_nation_supplier_nation_5.sum(lambda x: {x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]})
    
    lineitem_orders_customer_nation_supplier_nation_7 = lineitem_orders_customer_nation_supplier_nation_6.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_orders_customer_nation_supplier_nation_8 = lineitem_orders_customer_nation_supplier_nation_7.sum(lambda x: {record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record({"revenue": x[0].volume})})
    
    lineitem_orders_customer_nation_supplier_nation_9 = lineitem_orders_customer_nation_supplier_nation_8.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_customer_nation_supplier_nation_10 = lineitem_orders_customer_nation_supplier_nation_9.sum(lambda x: {x[0]: {record({"revenue": x[0].revenue}): True}})
    
    results = lineitem_orders_customer_nation_supplier_nation_10.sum(lambda x: x[1])
    
    # Complete

    return results
