from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    r = "R"
    customer_part = cu.sum(lambda x_customer: {x_customer[0].c_custkey: record({"c_custkey": x_customer[0].c_custkey, "c_name": x_customer[0].c_name, "c_address": x_customer[0].c_address, "c_nationkey": x_customer[0].c_nationkey, "c_phone": x_customer[0].c_phone, "c_acctbal": x_customer[0].c_acctbal, "c_mktsegment": x_customer[0].c_mktsegment, "c_comment": x_customer[0].c_comment})})
    
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_orderkey: record({"l_orderkey": x_lineitem[0].l_orderkey, "l_partkey": x_lineitem[0].l_partkey, "l_suppkey": x_lineitem[0].l_suppkey, "l_linenumber": x_lineitem[0].l_linenumber, "l_quantity": x_lineitem[0].l_quantity, "l_extendedprice": x_lineitem[0].l_extendedprice, "l_discount": x_lineitem[0].l_discount, "l_tax": x_lineitem[0].l_tax, "l_returnflag": x_lineitem[0].l_returnflag, "l_linestatus": x_lineitem[0].l_linestatus, "l_shipdate": x_lineitem[0].l_shipdate, "l_commitdate": x_lineitem[0].l_commitdate, "l_receiptdate": x_lineitem[0].l_receiptdate, "l_shipinstruct": x_lineitem[0].l_shipinstruct, "l_shipmode": x_lineitem[0].l_shipmode, "l_comment": x_lineitem[0].l_comment})}) if (x_lineitem[0].l_returnflag == r) else (None))
    
    customer_lineitem_orders = ord.sum(lambda x_orders: (({x_orders[0].c_nationkey: record({"c_acctbal": customer_part[x_orders[0].o_custkey].c_acctbal, "c_address": customer_part[x_orders[0].o_custkey].c_address, "c_comment": customer_part[x_orders[0].o_custkey].c_comment, "c_name": customer_part[x_orders[0].o_custkey].c_name, "c_phone": customer_part[x_orders[0].o_custkey].c_phone, "l_discount": lineitem_part[x_orders[0].o_orderkey].l_discount, "l_extendedprice": lineitem_part[x_orders[0].o_orderkey].l_extendedprice})}) if (lineitem_part[x_orders[0].o_orderkey] != None) else (None)) if (((x_orders[0].o_orderdate >= 19931001) * (x_orders[0].o_orderdate < 19940101))) else (None))
    
    nation_aggr = na.sum(lambda x_nation: ({record({"c_custkey": customer_lineitem_orders[x_nation[0].n_nationkey].c_custkey, "n_name": x_nation[0].n_name}): ((x_nation[0].l_extendedprice) * (((1) - (x_nation[0].l_discount))))}) if (customer_lineitem_orders[x_nation[0].n_nationkey] != None) else (None))
    
    results = nation_aggr.sum(lambda x_nation_aggr: {record({"c_custkey": x_nation_aggr[0].c_custkey, "n_name": x_nation_aggr[0].n_name, "revenue": x_nation_aggr[1]}): True})
    
    # Complete

    return results