from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    nation_part = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: record({"n_name": x_nation[0].n_name})}) if (((x_nation[0].n_name == france) + (x_nation[0].n_name == germany))) else (None))
    
    nation_supplier = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: record({"n1_name": nation_part[x_supplier[0].s_nationkey].n_name})}) if (nation_part[x_supplier[0].s_nationkey] != None) else (None))
    
    nation_customer = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: record({"n_name": nation_part[x_customer[0].c_nationkey].n_name})}) if (nation_part[x_customer[0].c_nationkey] != None) else (None))
    
    nation_customer_orders = ord.sum(lambda x_orders: ({x_orders[0].o_orderkey: record({"n2_name": nation_customer[x_orders[0].o_custkey].n_name})}) if (nation_customer[x_orders[0].o_custkey] != None) else (None))
    
    nation_supplier_nation_customer_orders_lineitem = li.sum(lambda x_lineitem: (((({record({"supp_nation": nation_supplier[x_lineitem[0].l_suppkey].n1_name, "cust_nation": nation_customer_orders[x_lineitem[0].l_orderkey].n2_name, "l_year": extractYear(x_lineitem[0].l_shipdate)}): record({"revenue": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})}) if (((((nation_supplier[x_lineitem[0].l_suppkey].n1_name == france) * (nation_customer_orders[x_lineitem[0].l_orderkey].n2_name == germany))) + (((nation_supplier[x_lineitem[0].l_suppkey].n1_name == germany) * (nation_customer_orders[x_lineitem[0].l_orderkey].n2_name == france))))) else (None)) if (nation_supplier[x_lineitem[0].l_suppkey] != None) else (None)) if (nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (((x_lineitem[0].l_shipdate >= 19950101) * (x_lineitem[0].l_shipdate <= 19961231))) else (None))
    
    results = nation_supplier_nation_customer_orders_lineitem.sum(lambda x_nation_supplier_nation_customer_orders_lineitem: {record({"supp_nation": x_nation_supplier_nation_customer_orders_lineitem[0].supp_nation, "cust_nation": x_nation_supplier_nation_customer_orders_lineitem[0].cust_nation, "l_year": x_nation_supplier_nation_customer_orders_lineitem[0].l_year, "revenue": x_nation_supplier_nation_customer_orders_lineitem[1].revenue}): True})
    
    # Complete

    return results
