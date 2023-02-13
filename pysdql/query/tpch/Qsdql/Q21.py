from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "l2": LINEITEM_TYPE, "l3": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE})
def query(su, li, l2, l3, ord, na):

    # Insert
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    orders_part = ord.sum(lambda x_orders: ({x_orders[0].o_orderkey: True}) if (x_orders[0].o_orderstatus == f) else (None))
    
    nation_part = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: True}) if (x_nation[0].n_name == saudiarabia) else (None))
    
    nation_supplier = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: record({"s_name": x_supplier[0].s_name})}) if (nation_part[x_supplier[0].s_nationkey] != None) else (None))
    
    l3_part = l3.sum(lambda x_l3: ({x_l3[0].l_orderkey: record({"l3_size": 1})}) if (x_l3[0].l_receiptdate > x_l3[0].l_commitdate) else (None))
    
    l2_part = l2.sum(lambda x_l2: {x_l2[0].l_orderkey: record({"l2_size": 1})})
    
    orders_nation_supplier_l3_l2_lineitem = li.sum(lambda x_lineitem: (((((({nation_supplier[x_lineitem[0].l_suppkey].s_name: record({"numwait": 1})}) if (((l2_part[x_lineitem[0].l_orderkey].l2_size > 1) * (l3_part[x_lineitem[0].l_orderkey].l3_size == 1))) else (None)) if (orders_part[x_lineitem[0].l_orderkey] != None) else (None)) if (nation_supplier[x_lineitem[0].l_suppkey] != None) else (None)) if (l3_part[x_lineitem[0].l_orderkey] != None) else (None)) if (l2_part[x_lineitem[0].l_orderkey] != None) else (None)) if (x_lineitem[0].l_receiptdate > x_lineitem[0].l_commitdate) else (None))
    
    results = orders_nation_supplier_l3_l2_lineitem.sum(lambda x_orders_nation_supplier_l3_l2_lineitem: {record({"s_name": x_orders_nation_supplier_l3_l2_lineitem[0], "numwait": x_orders_nation_supplier_l3_l2_lineitem[1].numwait}): True})
    
    # Complete

    return results
