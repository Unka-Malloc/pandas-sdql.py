from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE, REGION_TYPE, NATION_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "re": REGION_TYPE, "na": NATION_TYPE, "su": SUPPLIER_TYPE})
def query(li, cu, ord, re, na, su):

    # Insert
    middleeast = "MIDDLE EAST"
    region_part = re.sum(lambda x_region: ({x_region[0].r_regionkey: True}) if (x_region[0].r_name == middleeast) else (None))
    
    region_nation = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: record({"n_name": x_nation[0].n_name})}) if (region_part[x_nation[0].n_regionkey] != None) else (None))
    
    region_nation_customer = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: record({"c_nationkey": x_customer[0].c_nationkey, "n_name": region_nation[x_customer[0].c_nationkey].n_name})}) if (region_nation[x_customer[0].c_nationkey] != None) else (None))
    
    region_nation_customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record({"c_nationkey": region_nation_customer[x_orders[0].o_custkey].c_nationkey, "n_name": region_nation_customer[x_orders[0].o_custkey].n_name})}) if (region_nation_customer[x_orders[0].o_custkey] != None) else (None)) if (((x_orders[0].o_orderdate >= 19950101) * (x_orders[0].o_orderdate < 19960101))) else (None))
    
    supplier_part = su.sum(lambda x_supplier: {record({"s_suppkey": x_supplier[0].s_suppkey, "s_nationkey": x_supplier[0].s_nationkey}): True})
    
    supplier_region_nation_customer_orders_lineitem = li.sum(lambda x_lineitem: (({region_nation_customer_orders[x_lineitem[0].l_orderkey].n_name: record({"revenue": ((x_lineitem[0].l_extendedprice) * (((1) - (x_lineitem[0].l_discount))))})}) if (supplier_part[record({"l_suppkey": x_lineitem[0].l_suppkey, "c_nationkey": region_nation_customer_orders[x_lineitem[0].l_orderkey].c_nationkey})] != None) else (None)) if (region_nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None))
    
    results = supplier_region_nation_customer_orders_lineitem.sum(lambda x_supplier_region_nation_customer_orders_lineitem: {record({"n_name": x_supplier_region_nation_customer_orders_lineitem[0], "revenue": x_supplier_region_nation_customer_orders_lineitem[1].revenue}): True})
    
    # Complete

    return results
