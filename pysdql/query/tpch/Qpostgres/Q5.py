from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE, REGION_TYPE, NATION_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "re": REGION_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE})
def query(li, cu, ord, re, na, su):

    # Insert
    asia = "ASIA"
    lineitem_part = li.sum(lambda x_lineitem: {x_lineitem[0].l_suppkey: record({"l_orderkey": x_lineitem[0].l_orderkey, "l_partkey": x_lineitem[0].l_partkey, "l_suppkey": x_lineitem[0].l_suppkey, "l_linenumber": x_lineitem[0].l_linenumber, "l_quantity": x_lineitem[0].l_quantity, "l_extendedprice": x_lineitem[0].l_extendedprice, "l_discount": x_lineitem[0].l_discount, "l_tax": x_lineitem[0].l_tax, "l_returnflag": x_lineitem[0].l_returnflag, "l_linestatus": x_lineitem[0].l_linestatus, "l_shipdate": x_lineitem[0].l_shipdate, "l_commitdate": x_lineitem[0].l_commitdate, "l_receiptdate": x_lineitem[0].l_receiptdate, "l_shipinstruct": x_lineitem[0].l_shipinstruct, "l_shipmode": x_lineitem[0].l_shipmode, "l_comment": x_lineitem[0].l_comment})})
    
    supplier_part = su.sum(lambda x_supplier: {x_supplier[0].s_nationkey: record({"s_suppkey": x_supplier[0].s_suppkey, "s_name": x_supplier[0].s_name, "s_address": x_supplier[0].s_address, "s_nationkey": x_supplier[0].s_nationkey, "s_phone": x_supplier[0].s_phone, "s_acctbal": x_supplier[0].s_acctbal, "s_comment": x_supplier[0].s_comment})})
    
    nation_part = na.sum(lambda x_nation: {x_nation[0].n_regionkey: record({"n_nationkey": x_nation[0].n_nationkey, "n_name": x_nation[0].n_name, "n_regionkey": x_nation[0].n_regionkey, "n_comment": x_nation[0].n_comment})})
    
    lineitem_supplier_nation_region = re.sum(lambda x_region: ((((({lineitem_part[supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey].s_suppkey].l_orderkey: record({"l_extendedprice": lineitem_part[supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey].s_suppkey].l_extendedprice, "l_discount": lineitem_part[supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey].s_suppkey].l_discount, "l_orderkey": lineitem_part[supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey].s_suppkey].l_orderkey, "s_nationkey": supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey].s_nationkey, "n_name": nation_part[x_region[0].r_regionkey].n_name, "n_nationkey": nation_part[x_region[0].r_regionkey].n_nationkey})}) if (lineitem_part[supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey].s_suppkey] != None) else (None)) if (nation_part[x_region[0].r_regionkey] != None) else (None)) if (supplier_part[nation_part[x_region[0].r_regionkey].n_nationkey] != None) else (None)) if (nation_part[x_region[0].r_regionkey] != None) else (None)) if (x_region[0].r_name == asia) else (None))
    
    lineitem_supplier_nation_region_orders = ord.sum(lambda x_orders: (({record({"o_custkey": x_orders[0].o_custkey, "s_nationkey": lineitem_supplier_nation_region[x_orders[0].o_orderkey].s_nationkey}): record({"l_discount": lineitem_supplier_nation_region[x_orders[0].o_orderkey].l_discount, "l_extendedprice": lineitem_supplier_nation_region[x_orders[0].o_orderkey].l_extendedprice, "n_name": lineitem_supplier_nation_region[x_orders[0].o_orderkey].n_name})}) if (lineitem_supplier_nation_region[x_orders[0].o_orderkey] != None) else (None)) if (((x_orders[0].o_orderdate >= 19940101) * (x_orders[0].o_orderdate < 19950101))) else (None))
    
    customer_aggr = cu.sum(lambda x_customer: ({x_customer[0].n_name: ((lineitem_supplier_nation_region_orders[record({"o_custkey": x_customer[0].c_custkey, "s_nationkey": x_customer[0].c_nationkey})].l_extendedprice) * (((1) - (lineitem_supplier_nation_region_orders[record({"o_custkey": x_customer[0].c_custkey, "s_nationkey": x_customer[0].c_nationkey})].l_discount))))}) if (lineitem_supplier_nation_region_orders[record({"c_custkey": x_customer[0].c_custkey, "c_nationkey": x_customer[0].c_nationkey})] != None) else (None))
    
    results = customer_aggr.sum(lambda x_customer_aggr: {record({"n_name": x_customer_aggr[0], "revenue": x_customer_aggr[1]}): True})
    
    # Complete

    return results
