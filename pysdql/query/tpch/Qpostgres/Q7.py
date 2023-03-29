from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    lineitem_supplier_nation_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None))
    
    supplier_nation_probe_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_probe_pre_ops = supplier_nation_probe_pre_ops.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_supplier_nation_build_nest_dict = lineitem_supplier_nation_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_0 = lineitem_supplier_nation_probe_pre_ops.sum(lambda x: (lineitem_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    lineitem_supplier_nation_1 = lineitem_supplier_nation_0.sum(lambda x: {x[0]: sr_dict({record({"l_shipdate": x[0].l_shipdate, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_orderkey": x[0].l_orderkey, "n_name_x": x[0].n_name}): True})})
    
    lineitem_supplier_nation_orders_customer_nation_build_pre_ops = lineitem_supplier_nation_1.sum(lambda x: x[1])
    
    customer_nation_probe_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == germany) + (x[0].n_name == france))) else (None))
    
    customer_nation_build_nest_dict = cu.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_nation_probe_pre_ops = customer_nation_probe_pre_ops.sum(lambda x: (customer_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_customer_nation_build_nest_dict = ord.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_nation_0 = orders_customer_nation_probe_pre_ops.sum(lambda x: (orders_customer_nation_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_customer_nation_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    orders_customer_nation_1 = orders_customer_nation_0.sum(lambda x: {x[0]: sr_dict({record({"o_orderkey": x[0].o_orderkey, "n_name_y": x[0].n_name}): True})})
    
    lineitem_supplier_nation_orders_customer_nation_probe_pre_ops = orders_customer_nation_1.sum(lambda x: x[1])
    
    lineitem_supplier_nation_orders_customer_nation_build_nest_dict = lineitem_supplier_nation_orders_customer_nation_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_supplier_nation_orders_customer_nation_0 = lineitem_supplier_nation_orders_customer_nation_probe_pre_ops.sum(lambda x: (lineitem_supplier_nation_orders_customer_nation_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_supplier_nation_orders_customer_nation_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_supplier_nation_orders_customer_nation_1 = lineitem_supplier_nation_orders_customer_nation_0.sum(lambda x: {x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_2 = lineitem_supplier_nation_orders_customer_nation_1.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].n_name_x == france) * (x[0].n_name_y == germany))) + (((x[0].n_name_x == germany) * (x[0].n_name_y == france))))) else (None))
    
    lineitem_supplier_nation_orders_customer_nation_3 = lineitem_supplier_nation_orders_customer_nation_2.sum(lambda x: {x[0].concat(record({"supp_nation": x[0].n_name_x})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_4 = lineitem_supplier_nation_orders_customer_nation_3.sum(lambda x: {x[0].concat(record({"cust_nation": x[0].n_name_y})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_5 = lineitem_supplier_nation_orders_customer_nation_4.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_supplier_nation_orders_customer_nation_6 = lineitem_supplier_nation_orders_customer_nation_5.sum(lambda x: {record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record({"revenue": x[0].volume})})
    
    lineitem_supplier_nation_orders_customer_nation_7 = lineitem_supplier_nation_orders_customer_nation_6.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_supplier_nation_orders_customer_nation_8 = lineitem_supplier_nation_orders_customer_nation_7.sum(lambda x: {x[0]: {record({"revenue": x[0].revenue}): True}})
    
    results = lineitem_supplier_nation_orders_customer_nation_8.sum(lambda x: x[1])
    
    # Complete

    return results
