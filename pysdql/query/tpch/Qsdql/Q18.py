from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "l1": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, l1, cu, ord):

    # Insert
    customer_orders_l1_probe = l1
    v0 = li.sum(lambda x: ({record({"l_orderkey": x[0].l_orderkey}): record({"sum_quantity": x[0].l_quantity})}) if (True) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(x[1]): True}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0]: x[1]}) if (x[0].sum_quantity > 300) else (None))
    
    orders_lineitem_isin_build = v2
    orders_lineitem_isin_build = orders_lineitem_isin_build.sum(lambda x: ({x[0].l_orderkey: True}) if (True) else (None))
    
    v0 = ord.sum(lambda x: (({x[0]: x[1]}) if (orders_lineitem_isin_build[x[0].o_orderkey] != None) else (None)) if (True) else (None))
    
    customer_orders_probe = v0
    customer_orders_part = cu
    build_side = customer_orders_part.sum(lambda x: ({x[0].c_custkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_custkey] != None) else (None))
    
    customer_orders_l1_part = v0
    build_side = customer_orders_l1_part.sum(lambda x: ({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = customer_orders_l1_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({record({"c_name": x[0].c_name, "c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey, "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice}): record({"sum_quantity": x[0].l_quantity})}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(x[1]): True}) if (True) else (None))
    
    results = v2
    # Complete

    return results
