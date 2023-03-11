from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):

    # Insert
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_orderkey: record})}) if (x_lineitem[0].suml_quantity > 300) else (None))
    
    lineitem_aggr = li.sum(lambda x_lineitem: {x_lineitem[0].l_orderkey: x_lineitem[0].l_quantity})
    
    orders_part = ord.sum(lambda x_orders: {x_orders[0].o_orderkey: record({"o_orderkey": x_orders[0].o_orderkey, "o_orderdate": x_orders[0].o_orderdate, "o_totalprice": x_orders[0].o_totalprice, "o_custkey": x_orders[0].o_custkey})})
    
    orders_lineitem = li.sum(lambda x_lineitem: (({x_lineitem[0].o_custkey: record({"l_orderkey": x_lineitem[0].l_orderkey, "o_orderdate": orders_part[x_lineitem[0].l_orderkey].o_orderdate, "o_orderkey": x_lineitem[0].l_orderkey, "o_totalprice": orders_part[x_lineitem[0].l_orderkey].o_totalprice})}) if (orders_part[x_lineitem[0].l_orderkey] != None) else (None)) if (x_lineitem[0].suml_quantity > 300) else (None))
    
    lineitem_orders_lineitem_customer = cu.sum(lambda x_customer: (({record({"c_custkey": x_customer[0].c_custkey, "o_orderkey": x_customer[0].o_orderkey}): True}) if (lineitem_part[orders_lineitem[x_customer[0].c_custkey].o_orderkey] != None) else (None)) if (orders_lineitem[x_customer[0].c_custkey] != None) else (None))
    
    results = lineitem_orders_lineitem_customer.sum(lambda x_lineitem_orders_lineitem_customer: {record({"suml_quantity": x_lineitem_orders_lineitem_customer[1].suml_quantity}): True})
    
    # Complete

    return results
