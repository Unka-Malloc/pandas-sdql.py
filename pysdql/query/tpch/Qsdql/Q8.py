from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE,
                                     NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE,
               "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, li, ord, cu, na, re):

    # Insert
    economyanodizedsteel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"
    n2_part = na.sum(lambda x_n2: {x_n2[0].n_nationkey: record({"n2_comment": x_n2[0].n_comment, "n2_name": x_n2[0].n_name, "n2_nationkey": x_n2[0].n_nationkey, "n2_regionkey": x_n2[0].n_regionkey})})
    
    supplier_part = su.sum(lambda x_supplier: {x_supplier[0].s_suppkey: record({"s_nationkey": x_supplier[0].s_nationkey})})
    
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: True}) if (x_part[0].p_type == economyanodizedsteel) else (None))
    
    region_part = re.sum(lambda x_region: ({x_region[0].r_regionkey: True}) if (x_region[0].r_name == america) else (None))
    
    region_nation = na.sum(lambda x_n1: ({x_n1[0].n_nationkey: record({"n1_nationkey": x_n1[0].n_nationkey})}) if (region_part[x_n1[0].n_regionkey] != None) else (None))
    
    region_nation_customer = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: True}) if (region_nation[x_customer[0].c_nationkey] != None) else (None))
    
    region_nation_customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record({"o_orderdate": x_orders[0].o_orderdate})}) if (region_nation_customer[x_orders[0].o_custkey] != None) else (None)) if (((x_orders[0].o_orderdate >= 19950101) * (x_orders[0].o_orderdate <= 19961231))) else (None))
    
    nation_supplier_part_region_nation_customer_orders_lineitem = li.sum(lambda x_lineitem: (((({extractYear(region_nation_customer_orders[x_lineitem[0].l_orderkey].o_orderdate): record({"A": (((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) if (n2_part[supplier_part[x_lineitem[0].l_suppkey].s_nationkey].n2_name == brazil) else (0.0), "B": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})}) if (n2_part[supplier_part[x_lineitem[0].l_suppkey].s_nationkey] != None) else (None)) if (supplier_part[x_lineitem[0].l_suppkey] != None) else (None)) if (part_part[x_lineitem[0].l_partkey] != None) else (None)) if (region_nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None))
    
    results = nation_supplier_part_region_nation_customer_orders_lineitem.sum(lambda x_nation_supplier_part_region_nation_customer_orders_lineitem: {record({"o_year": x_nation_supplier_part_region_nation_customer_orders_lineitem[0], "mkt_share": ((x_nation_supplier_part_region_nation_customer_orders_lineitem[1].A) / (x_nation_supplier_part_region_nation_customer_orders_lineitem[1].B))}): True})
    
    # Complete

    return results
