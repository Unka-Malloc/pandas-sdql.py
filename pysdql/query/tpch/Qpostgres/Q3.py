from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):
    # Insert
    building = "BUILDING"
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_orderkey: record({"l_orderkey": x_lineitem[0].l_orderkey, "l_extendedprice": x_lineitem[0].l_extendedprice, "l_discount": x_lineitem[0].l_discount})}) if (x_lineitem[0].l_shipdate > 19950315) else (None))
    
    orders_part = ord.sum(lambda x_orders: ({x_orders[0].o_custkey: record({"o_orderkey": x_orders[0].o_orderkey, "o_custkey": x_orders[0].o_custkey, "o_orderstatus": x_orders[0].o_orderstatus, "o_totalprice": x_orders[0].o_totalprice, "o_orderdate": x_orders[0].o_orderdate, "o_orderpriority": x_orders[0].o_orderpriority, "o_clerk": x_orders[0].o_clerk, "o_shippriority": x_orders[0].o_shippriority, "o_comment": x_orders[0].o_comment})}) if (x_orders[0].o_orderdate < 19950315) else (None))
    
    lineitem_orders_customer = cu.sum(lambda x_customer: ((((({record({"l_orderkey": orders_part[x_customer[0].c_custkey].o_orderkey, "o_orderdate": orders_part[x_customer[0].c_custkey].o_orderdate, "o_shippriority": orders_part[x_customer[0].c_custkey].o_shippriority}): record({"revenue": ((lineitem_part[orders_part[x_customer[0].c_custkey].o_orderkey].l_extendedprice) * (((1) - (lineitem_part[orders_part[x_customer[0].c_custkey].o_orderkey].l_discount))))})}) if (lineitem_part[orders_part[x_customer[0].c_custkey].o_orderkey]) else (None)) if (orders_part[x_customer[0].c_custkey]) else (None)) if (lineitem_part[orders_part[x_customer[0].c_custkey].o_orderkey] != None) else (None)) if (orders_part[x_customer[0].c_custkey] != None) else (None)) if (x_customer[0].c_mktsegment == building) else (None))
    
    results = lineitem_orders_customer.sum(lambda x_lineitem_orders_customer: {record({"l_orderkey": x_lineitem_orders_customer[0].l_orderkey,
                                                                                       "o_orderdate": x_lineitem_orders_customer[0].o_orderdate,
                                                                                       "o_shippriority": x_lineitem_orders_customer[0].o_shippriority,
                                                                                       "revenue": x_lineitem_orders_customer[1].revenue}):
                                                                                   True})
    
    # Complete

    return results
