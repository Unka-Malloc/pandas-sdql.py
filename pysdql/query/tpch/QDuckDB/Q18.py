from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):

    # Insert
    lineitem_orders_build_nest_dict = li.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_index = ord.sum(lambda x: (lineitem_orders_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"sum_l_quantity": x[0].l_quantity})})
    
    lineitem_2 = lineitem_1.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_3 = lineitem_2.sum(lambda x: {x[0].concat(record({"suml_quantity": x[0].sum_l_quantity})): x[1]})
    
    lineitem_4 = lineitem_3.sum(lambda x: ({x[0]: x[1]}) if (x[0].suml_quantity > 300.0) else (None))
    
    lineitem_orders_customer_build_nest_dict = lineitem_orders_customer_index.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_0 = cu.sum(lambda x: (lineitem_orders_customer_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_lineitem_orders_customer_isin_build_index = lineitem_4.sum(lambda x: {x[0].l_orderkey: True})
    
    lineitem_orders_customer_1 = lineitem_orders_customer_0.sum(lambda x: ({x[0]: x[1]}) if (lineitem_lineitem_orders_customer_isin_build_index[x[0].o_orderkey] != None) else (None))
    
    lineitem_orders_customer_2 = lineitem_orders_customer_1.sum(lambda x: {record({"c_name": x[0].c_name, "c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice}): record({"suml_quantity": x[0].l_quantity})})
    
    results = lineitem_orders_customer_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
