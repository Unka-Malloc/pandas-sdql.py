from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE,
                                     NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE,
               "na": NATION_TYPE, "n1": NATION_TYPE, "n2": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, li, ord, cu, na, n1, n2, re):

    # Insert
    economyanodizedsteel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"
    region_n1_customer_orders_lineitem_probe = li
    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19950101) * (x[0].o_orderdate <= 19961231))) else (None))
    
    region_n1_customer_orders_probe = v0
    region_n1_customer_probe = cu
    v0 = n1.sum(lambda x: ({x[0].concat(record({"n1_nationkey": x[0].n_nationkey})): x[1]}) if (True) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"n1_name": x[0].n_name})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"n1_regionkey": x[0].n_regionkey})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: ({x[0].concat(record({"n1_comment": x[0].n_comment})): x[1]}) if (True) else (None))
    
    region_n1_probe = v3
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == america) else (None))
    
    region_n1_part = v0
    build_side = region_n1_part.sum(lambda x: ({x[0].r_regionkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = region_n1_probe.sum(lambda x: ({build_side[x[0].n1_regionkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].n1_regionkey] != None) else (None))
    
    region_n1_customer_part = v0
    build_side = region_n1_customer_part.sum(lambda x: ({x[0].n1_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = region_n1_customer_probe.sum(lambda x: ({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].c_nationkey] != None) else (None))
    
    region_n1_customer_orders_part = v0
    build_side = region_n1_customer_orders_part.sum(lambda x: ({x[0].c_custkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = region_n1_customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_custkey] != None) else (None))
    
    region_n1_customer_orders_lineitem_part = v0
    build_side = region_n1_customer_orders_lineitem_part.sum(lambda x: ({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = region_n1_customer_orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None))
    
    part_region_n1_customer_orders_lineitem_probe = v0
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (x[0].p_type == economyanodizedsteel) else (None))
    
    part_region_n1_customer_orders_lineitem_part = v0
    build_side = part_region_n1_customer_orders_lineitem_part.sum(lambda x: ({x[0].p_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = part_region_n1_customer_orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_partkey] != None) else (None))
    
    supplier_part_region_n1_customer_orders_lineitem_probe = v0
    supplier_part_region_n1_customer_orders_lineitem_part = su
    build_side = supplier_part_region_n1_customer_orders_lineitem_part.sum(lambda x: ({x[0].s_suppkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = supplier_part_region_n1_customer_orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_suppkey] != None) else (None))
    
    n2_supplier_part_region_n1_customer_orders_lineitem_probe = v0
    v0 = n2.sum(lambda x: ({x[0].concat(record({"n2_nationkey": x[0].n_nationkey})): x[1]}) if (True) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"n2_name": x[0].n_name})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"n2_regionkey": x[0].n_regionkey})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: ({x[0].concat(record({"n2_comment": x[0].n_comment})): x[1]}) if (True) else (None))
    
    n2_supplier_part_region_n1_customer_orders_lineitem_part = v3
    build_side = n2_supplier_part_region_n1_customer_orders_lineitem_part.sum(lambda x: ({x[0].n2_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = n2_supplier_part_region_n1_customer_orders_lineitem_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]}) if (True) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: ({x[0].concat(record({"nation": x[0].n2_name})): x[1]}) if (True) else (None))
    
    v4 = v3.sum(lambda x: ({x[0].concat(record({"volume_A": (((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) if (x[0].nation == brazil) else (0.0)})): x[1]}) if (True) else (None))
    
    v5 = v4.sum(lambda x: ({record({"o_year": x[0].o_year}): record({"A": x[0].volume_A, "B": x[0].volume})}) if (True) else (None))
    
    v6 = v5.sum(lambda x: ({x[0].concat(x[1]): True}) if (True) else (None))
    
    v7 = v6.sum(lambda x: ({x[0].concat(record({"mkt_share": ((x[0].A) / (x[0].B))})): x[1]}) if (True) else (None))
    
    results = v7
    # Complete

    return results
