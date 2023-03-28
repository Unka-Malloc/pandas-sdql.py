from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None))
    
    lineitem_supplier_nation_build_pre_ops = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    nation_0 = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))
    
    supplier_nation_probe_pre_ops = nation_0.sum(lambda x: {record({"n_name": x[0].n_name, "n_nationkey": x[0].n_nationkey}): True})
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    supplier_nation_0 = supplier_nation_probe_pre_ops.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_supplier_nation_probe_pre_ops = supplier_nation_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "n_name": x[0].n_name}): True})
    
    lineitem_supplier_nation_build_nest_dict = lineitem_supplier_nation_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_0 = lineitem_supplier_nation_probe_pre_ops.sum(lambda x: (lineitem_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    lineitem_supplier_nation_orders_customer_nation_build_pre_ops = lineitem_supplier_nation_0.sum(lambda x: {record({"l_shipdate": x[0].l_shipdate, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_orderkey": x[0].l_orderkey, "n_name": x[0].n_name}): True})
    
    nation_0 = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == germany) + (x[0].n_name == france))) else (None))
    
    customer_nation_probe_pre_ops = nation_0.sum(lambda x: {record({"n_name": x[0].n_name, "n_nationkey": x[0].n_nationkey}): True})
    
    customer_nation_build_nest_dict = cu.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    customer_nation_0 = customer_nation_probe_pre_ops.sum(lambda x: (customer_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_customer_nation_probe_pre_ops = customer_nation_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey, "n_name": x[0].n_name}): True})
    
    orders_customer_nation_build_nest_dict = ord.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_nation_0 = orders_customer_nation_probe_pre_ops.sum(lambda x: (orders_customer_nation_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_customer_nation_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_supplier_nation_orders_customer_nation_probe_pre_ops = orders_customer_nation_0.sum(lambda x: {record({"o_orderkey": x[0].o_orderkey, "n_name": x[0].n_name}): True})
    
    lineitem_supplier_nation_orders_customer_nation_build_nest_dict = lineitem_supplier_nation_orders_customer_nation_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_orders_customer_nation_0 = lineitem_supplier_nation_orders_customer_nation_probe_pre_ops.sum(lambda x: (lineitem_supplier_nation_orders_customer_nation_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_orders_customer_nation_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_supplier_nation_orders_customer_nation_1 = lineitem_supplier_nation_orders_customer_nation_0.sum(lambda x: {x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_2 = lineitem_supplier_nation_orders_customer_nation_1.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].n_name_x == france) * (x[0].n_name_y == germany))) + (((x[0].n_name_x == germany) * (x[0].n_name_y == france))))) else (None))
    
    lineitem_supplier_nation_orders_customer_nation_3 = lineitem_supplier_nation_orders_customer_nation_2.sum(lambda x: {record({"n_name_x": x[0].n_name_x, "n_name_y": x[0].n_name_y, "l_year": x[0].l_year, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    lineitem_supplier_nation_orders_customer_nation_3 = lineitem_supplier_nation_orders_customer_nation_2.sum(lambda x: {x[0].concat(record({"supp_nation": x[0].n_name_x})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_4 = lineitem_supplier_nation_orders_customer_nation_3.sum(lambda x: {x[0].concat(record({"cust_nation": x[0].n_name_y})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_5 = lineitem_supplier_nation_orders_customer_nation_4.sum(lambda x: {record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount}): True})
    
    lineitem_supplier_nation_orders_customer_nation_5 = lineitem_supplier_nation_orders_customer_nation_4.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_6 = lineitem_supplier_nation_orders_customer_nation_5.sum(lambda x: {record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record({"revenue": x[0].volume})})
    
    lineitem_supplier_nation_orders_customer_nation_7 = lineitem_supplier_nation_orders_customer_nation_6.sum(lambda x: {x[0].concat(x[1]): True})
    
    results = lineitem_supplier_nation_orders_customer_nation_7.sum(lambda x: {record({"revenue": x[0].revenue}): True})
    
    # Complete

    return results
