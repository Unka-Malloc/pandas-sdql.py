from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    n1_supplier_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))
    
    n1_supplier_build_nest_dict = n1_supplier_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    n1_supplier_0 = su.sum(lambda x: (n1_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (n1_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    n1_supplier_n2_customer_orders_lineitem_build_pre_ops = n1_supplier_0.sum(lambda x: {x[0].concat(record({"n1_name": x[0].n_name})): x[1]})
    
    n2_customer_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))
    
    n2_customer_build_nest_dict = n2_customer_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    n2_customer_orders_build_pre_ops = cu.sum(lambda x: (n2_customer_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (n2_customer_build_nest_dict[x[0].c_nationkey] != None) else (None))
    
    n2_customer_orders_build_nest_dict = n2_customer_orders_build_pre_ops.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    n2_customer_orders_0 = ord.sum(lambda x: (n2_customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (n2_customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))
    
    n2_customer_orders_lineitem_build_pre_ops = n2_customer_orders_0.sum(lambda x: {x[0].concat(record({"n2_name": x[0].n_name})): x[1]})
    
    n2_customer_orders_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None))
    
    n2_customer_orders_lineitem_build_nest_dict = n2_customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    n1_supplier_n2_customer_orders_lineitem_probe_pre_ops = n2_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (n2_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (n2_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    n1_supplier_n2_customer_orders_lineitem_build_nest_dict = n1_supplier_n2_customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    n1_supplier_n2_customer_orders_lineitem_0 = n1_supplier_n2_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (n1_supplier_n2_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (n1_supplier_n2_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))
    
    n1_supplier_n2_customer_orders_lineitem_1 = n1_supplier_n2_customer_orders_lineitem_0.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].n1_name == france) * (x[0].n2_name == germany))) + (((x[0].n1_name == germany) * (x[0].n2_name == france))))) else (None))
    
    n1_supplier_n2_customer_orders_lineitem_2 = n1_supplier_n2_customer_orders_lineitem_1.sum(lambda x: {x[0].concat(record({"supp_nation": x[0].n1_name})): x[1]})
    
    n1_supplier_n2_customer_orders_lineitem_3 = n1_supplier_n2_customer_orders_lineitem_2.sum(lambda x: {x[0].concat(record({"cust_nation": x[0].n2_name})): x[1]})
    
    n1_supplier_n2_customer_orders_lineitem_4 = n1_supplier_n2_customer_orders_lineitem_3.sum(lambda x: {x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]})
    
    n1_supplier_n2_customer_orders_lineitem_5 = n1_supplier_n2_customer_orders_lineitem_4.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    n1_supplier_n2_customer_orders_lineitem_6 = n1_supplier_n2_customer_orders_lineitem_5.sum(lambda x: {record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year, "volume": x[0].volume}): True})
    
    n1_supplier_n2_customer_orders_lineitem_6 = n1_supplier_n2_customer_orders_lineitem_5.sum(lambda x: {record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record({"revenue": x[0].volume})})
    
    results = n1_supplier_n2_customer_orders_lineitem_6.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
