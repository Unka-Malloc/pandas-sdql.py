from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    r = "R"
    nation_part = na.sum(lambda x_nation: {x_nation[0].n_nationkey: record({"n_name": x_nation[0].n_name})})
    
    customer_part = cu.sum(lambda x_customer: {x_customer[0].c_custkey: record({"c_acctbal": x_customer[0].c_acctbal, "c_address": x_customer[0].c_address, "c_comment": x_customer[0].c_comment, "c_custkey": x_customer[0].c_custkey, "c_name": x_customer[0].c_name, "c_phone": x_customer[0].c_phone, "c_nationkey": x_customer[0].c_nationkey})})
    
    nation_customer_orders = ord.sum(lambda x_orders: (((({x_orders[0].o_orderkey: record({"o_orderkey": x_orders[0].o_orderkey, "c_custkey": x_orders[0].o_custkey, "c_name": customer_part[x_orders[0].o_custkey].c_name, "c_acctbal": customer_part[x_orders[0].o_custkey].c_acctbal, "c_phone": customer_part[x_orders[0].o_custkey].c_phone, "n_name": nation_part[customer_part[x_orders[0].o_custkey].c_nationkey].n_name, "c_address": customer_part[x_orders[0].o_custkey].c_address, "c_comment": customer_part[x_orders[0].o_custkey].c_comment})}) if (customer_part[x_orders[0].o_custkey] != None) else (None)) if (nation_part[customer_part[x_orders[0].o_custkey].c_nationkey] != None) else (None)) if (customer_part[x_orders[0].o_custkey] != None) else (None)) if (((x_orders[0].o_orderdate >= 19931001) * (x_orders[0].o_orderdate < 19940101))) else (None))
    
    lineitem_aggr = li.sum(lambda x_lineitem: (({record({"c_custkey": nation_customer_orders[x_lineitem[0].l_orderkey].c_custkey, "c_name": nation_customer_orders[x_lineitem[0].l_orderkey].c_name, "c_acctbal": nation_customer_orders[x_lineitem[0].l_orderkey].c_acctbal, "c_phone": nation_customer_orders[x_lineitem[0].l_orderkey].c_phone, "n_name": nation_customer_orders[x_lineitem[0].l_orderkey].n_name, "c_address": nation_customer_orders[x_lineitem[0].l_orderkey].c_address, "c_comment": nation_customer_orders[x_lineitem[0].l_orderkey].c_comment}): ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))}) if (nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (x_lineitem[0].l_returnflag == r) else (None))
    
    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record({"c_custkey": x_lineitem_aggr[0].c_custkey, "c_name": x_lineitem_aggr[0].c_name, "c_acctbal": x_lineitem_aggr[0].c_acctbal, "c_phone": x_lineitem_aggr[0].c_phone, "n_name": x_lineitem_aggr[0].n_name, "c_address": x_lineitem_aggr[0].c_address, "c_comment": x_lineitem_aggr[0].c_comment, "revenue": x_lineitem_aggr[1]}): True})
    
    # Complete

    return results