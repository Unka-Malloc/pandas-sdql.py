from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):

    # Insert
    customer_part = cu.sum(lambda x_customer: {x_customer[0].c_custkey: record({"c_custkey": x_customer[0].c_custkey, "c_name": x_customer[0].c_name})})
    
    lineitem_aggr = li.sum(lambda x_lineitem: {x_lineitem[0].l_orderkey: x_lineitem[0].l_quantity})
    
    lineitem_part = lineitem_aggr.sum(lambda x_lineitem_aggr: ({x_lineitem_aggr[0]: True}) if (x_lineitem_aggr[1] > 300) else (None))
    
    customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record({"c_custkey": x_orders[0].o_custkey, "c_name": customer_part[x_orders[0].o_custkey].c_name, "o_orderdate": x_orders[0].o_orderdate, "o_orderkey": x_orders[0].o_orderkey, "o_totalprice": x_orders[0].o_totalprice})}) if (customer_part[x_orders[0].o_custkey] != None) else (None)) if (lineitem_part[x_orders[0].o_orderkey] != None) else (None))
    
    l1_aggr = li.sum(lambda x_l1: ({record({"c_name": customer_orders[x_l1[0].l_orderkey].c_name, "c_custkey": customer_orders[x_l1[0].l_orderkey].c_custkey, "o_orderkey": x_l1[0].l_orderkey, "o_orderdate": customer_orders[x_l1[0].l_orderkey].o_orderdate, "o_totalprice": customer_orders[x_l1[0].l_orderkey].o_totalprice}): x_l1[0].l_quantity}) if (customer_orders[x_l1[0].l_orderkey] != None) else (None))
    
    results = l1_aggr.sum(lambda x_l1_aggr: {record({"c_name": x_l1_aggr[0].c_name, "c_custkey": x_l1_aggr[0].c_custkey, "o_orderkey": x_l1_aggr[0].o_orderkey, "o_orderdate": x_l1_aggr[0].o_orderdate, "o_totalprice": x_l1_aggr[0].o_totalprice, "sum_quantity": x_l1_aggr[1]}): True})
    
    # Complete

    return results
