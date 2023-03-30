from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    n1_part = na.sum(lambda x_n1: ({x_n1[0].n_nationkey: record({"n_name": x_n1[0].n_name})}) if (((x_n1[0].n_name == france) + (x_n1[0].n_name == germany))) else (None))
    
    n1_supplier = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: record({"n1_name": n1_part[x_supplier[0].s_nationkey].n_name})}) if (n1_part[x_supplier[0].s_nationkey] != None) else (None))
    
    n2_part = na.sum(lambda x_n2: ({x_n2[0].n_nationkey: record({"n_name": x_n2[0].n_name})}) if (((x_n2[0].n_name == france) + (x_n2[0].n_name == germany))) else (None))
    
    n2_customer = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: record({"n_name": n2_part[x_customer[0].c_nationkey].n_name})}) if (n2_part[x_customer[0].c_nationkey] != None) else (None))
    
    n2_customer_orders = ord.sum(lambda x_orders: ({x_orders[0].o_orderkey: record({"n2_name": n2_customer[x_orders[0].o_custkey].n_name})}) if (n2_customer[x_orders[0].o_custkey] != None) else (None))
    
    n1_supplier_n2_customer_orders_lineitem = li.sum(lambda x_lineitem: (((({record({"supp_nation": n1_supplier[x_lineitem[0].l_suppkey].n1_name, "cust_nation": n2_customer_orders[x_lineitem[0].l_orderkey].n2_name, "l_year": extractYear(x_lineitem[0].l_shipdate)}): record({"revenue": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})}) if (((((n1_supplier[x_lineitem[0].l_suppkey].n1_name == france) * (n2_customer_orders[x_lineitem[0].l_orderkey].n2_name == germany))) + (((n1_supplier[x_lineitem[0].l_suppkey].n1_name == germany) * (n2_customer_orders[x_lineitem[0].l_orderkey].n2_name == france))))) else (None)) if (n1_supplier[x_lineitem[0].l_suppkey] != None) else (None)) if (n2_customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (((x_lineitem[0].l_shipdate >= 19950101) * (x_lineitem[0].l_shipdate <= 19961231))) else (None))
    
    results = n1_supplier_n2_customer_orders_lineitem.sum(lambda x_n1_supplier_n2_customer_orders_lineitem: {record({"supp_nation": x_n1_supplier_n2_customer_orders_lineitem[0].supp_nation, "cust_nation": x_n1_supplier_n2_customer_orders_lineitem[0].cust_nation, "l_year": x_n1_supplier_n2_customer_orders_lineitem[0].l_year, "revenue": x_n1_supplier_n2_customer_orders_lineitem[1].revenue}): True})
    
    # Complete

    return results
