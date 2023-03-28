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
    n2_0 = na.sum(lambda x: {x[0].concat(record({"n2_nationkey": x[0].n_nationkey})): x[1]})
    
    n2_1 = n2_0.sum(lambda x: {x[0].concat(record({"n2_name": x[0].n_name})): x[1]})
    
    n2_2 = n2_1.sum(lambda x: {x[0].concat(record({"n2_regionkey": x[0].n_regionkey})): x[1]})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_build_pre_ops = n2_2.sum(lambda x: {x[0].concat(record({"n2_comment": x[0].n_comment})): x[1]})
    
    part_region_n1_customer_orders_lineitem_build_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (x[0].p_type == economyanodizedsteel) else (None))
    
    region_n1_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == america) else (None))
    
    n1_0 = na.sum(lambda x: {x[0].concat(record({"n1_nationkey": x[0].n_nationkey})): x[1]})
    
    n1_1 = n1_0.sum(lambda x: {x[0].concat(record({"n1_name": x[0].n_name})): x[1]})
    
    n1_2 = n1_1.sum(lambda x: {x[0].concat(record({"n1_regionkey": x[0].n_regionkey})): x[1]})
    
    region_n1_probe_pre_ops = n1_2.sum(lambda x: {x[0].concat(record({"n1_comment": x[0].n_comment})): x[1]})
    
    region_n1_build_nest_dict = region_n1_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})
    
    region_n1_customer_build_pre_ops = region_n1_probe_pre_ops.sum(lambda x: (region_n1_build_nest_dict[x[0].n1_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_n1_build_nest_dict[x[0].n1_regionkey] != None) else (None))
    
    region_n1_customer_build_nest_dict = region_n1_customer_build_pre_ops.sum(lambda x: {x[0].n1_nationkey: sr_dict({x[0]: x[1]})})
    
    region_n1_customer_orders_build_pre_ops = cu.sum(lambda x: (region_n1_customer_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_n1_customer_build_nest_dict[x[0].c_nationkey] != None) else (None))
    
    region_n1_customer_orders_probe_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19950101) * (x[0].o_orderdate <= 19961231))) else (None))
    
    region_n1_customer_orders_build_nest_dict = region_n1_customer_orders_build_pre_ops.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})
    
    region_n1_customer_orders_lineitem_build_pre_ops = region_n1_customer_orders_probe_pre_ops.sum(lambda x: (region_n1_customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_n1_customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))
    
    region_n1_customer_orders_lineitem_build_nest_dict = region_n1_customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    part_region_n1_customer_orders_lineitem_probe_pre_ops = li.sum(lambda x: (region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    part_region_n1_customer_orders_lineitem_build_nest_dict = part_region_n1_customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops = part_region_n1_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))
    
    supplier_part_region_n1_customer_orders_lineitem_build_nest_dict = su.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops = supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))
    
    n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict = n2_supplier_part_region_n1_customer_orders_lineitem_build_pre_ops.sum(lambda x: {x[0].n2_nationkey: sr_dict({x[0]: x[1]})})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_0 = n2_supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    n2_supplier_part_region_n1_customer_orders_lineitem_1 = n2_supplier_part_region_n1_customer_orders_lineitem_0.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_2 = n2_supplier_part_region_n1_customer_orders_lineitem_1.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_3 = n2_supplier_part_region_n1_customer_orders_lineitem_2.sum(lambda x: {x[0].concat(record({"nation": x[0].n2_name})): x[1]})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_4 = n2_supplier_part_region_n1_customer_orders_lineitem_3.sum(lambda x: {record({"o_year": x[0].o_year, "volume": x[0].volume, "nation": x[0].nation}): True})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_4 = n2_supplier_part_region_n1_customer_orders_lineitem_3.sum(lambda x: {x[0].concat(record({"volume_A": (x[0].volume) if (x[0].nation == brazil) else (0.0)})): x[1]})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_5 = n2_supplier_part_region_n1_customer_orders_lineitem_4.sum(lambda x: {record({"o_year": x[0].o_year}): record({"A": x[0].volume_A, "B": x[0].volume})})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_6 = n2_supplier_part_region_n1_customer_orders_lineitem_5.sum(lambda x: {x[0].concat(x[1]): True})
    
    n2_supplier_part_region_n1_customer_orders_lineitem_7 = n2_supplier_part_region_n1_customer_orders_lineitem_6.sum(lambda x: {x[0].concat(record({"mkt_share": ((x[0].A) / (x[0].B))})): x[1]})
    
    results = n2_supplier_part_region_n1_customer_orders_lineitem_7.sum(lambda x: {record({"o_year": x[0].o_year, "mkt_share": x[0].mkt_share}): True})
    
    # Complete

    return results
