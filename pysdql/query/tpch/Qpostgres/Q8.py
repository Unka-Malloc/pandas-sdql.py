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
    orders_lineitem_part_build_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19950101) * (x[0].o_orderdate <= 19961231))) else (None))
    
    part_0 = pa.sum(lambda x: ({x[0]: x[1]}) if (x[0].p_type == economyanodizedsteel) else (None))
    
    lineitem_part_probe_pre_ops = part_0.sum(lambda x: {record({"p_partkey": x[0].p_partkey}): True})
    
    lineitem_part_build_nest_dict = li.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_probe_pre_ops = lineitem_part_probe_pre_ops.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    orders_lineitem_part_build_nest_dict = orders_lineitem_part_build_pre_ops.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_customer_nation_region_build_pre_ops = orders_lineitem_part_probe_pre_ops.sum(lambda x: (orders_lineitem_part_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_build_nest_dict[x[0].l_orderkey] != None) else (None))
    
    region_0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == america) else (None))
    
    nation_region_probe_pre_ops = region_0.sum(lambda x: {record({"r_regionkey": x[0].r_regionkey}): True})
    
    nation_region_build_nest_dict = na.sum(lambda x: {x[0].n_regionkey: sr_dict({x[0]: x[1]})})
    
    nation_region_0 = nation_region_probe_pre_ops.sum(lambda x: (nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    customer_nation_region_probe_pre_ops = nation_region_0.sum(lambda x: {record({"n_nationkey": x[0].n_nationkey}): True})
    
    customer_nation_region_build_nest_dict = cu.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    customer_nation_region_0 = customer_nation_region_probe_pre_ops.sum(lambda x: (customer_nation_region_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_nation_region_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_lineitem_part_customer_nation_region_probe_pre_ops = customer_nation_region_0.sum(lambda x: {record({"c_custkey": x[0].c_custkey}): True})
    
    orders_lineitem_part_customer_nation_region_build_nest_dict = orders_lineitem_part_customer_nation_region_build_pre_ops.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_customer_nation_region_supplier_build_pre_ops = orders_lineitem_part_customer_nation_region_probe_pre_ops.sum(lambda x: (orders_lineitem_part_customer_nation_region_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_customer_nation_region_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    orders_lineitem_part_customer_nation_region_supplier_build_nest_dict = orders_lineitem_part_customer_nation_region_supplier_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_build_pre_ops = su.sum(lambda x: (orders_lineitem_part_customer_nation_region_supplier_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_customer_nation_region_supplier_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    orders_lineitem_part_customer_nation_region_supplier_nation_build_nest_dict = orders_lineitem_part_customer_nation_region_supplier_nation_build_pre_ops.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_0 = na.sum(lambda x: (orders_lineitem_part_customer_nation_region_supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_lineitem_part_customer_nation_region_supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_lineitem_part_customer_nation_region_supplier_nation_1 = orders_lineitem_part_customer_nation_region_supplier_nation_0.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_2 = orders_lineitem_part_customer_nation_region_supplier_nation_1.sum(lambda x: {x[0].concat(record({"case_a": (((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))) if (x[0].n_name == brazil) else (0)})): x[1]})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_3 = orders_lineitem_part_customer_nation_region_supplier_nation_2.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_4 = orders_lineitem_part_customer_nation_region_supplier_nation_3.sum(lambda x: {record({"o_year": x[0].o_year}): record({"sum_case_a": x[0].case_a, "sum_volume": x[0].volume})})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_5 = orders_lineitem_part_customer_nation_region_supplier_nation_4.sum(lambda x: {x[0].concat(x[1]): True})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_6 = orders_lineitem_part_customer_nation_region_supplier_nation_5.sum(lambda x: {x[0].concat(record({"mkt_share": ((x[0].sum_case_a) / (x[0].sum_volume))})): x[1]})
    
    orders_lineitem_part_customer_nation_region_supplier_nation_7 = orders_lineitem_part_customer_nation_region_supplier_nation_6.sum(lambda x: {record({"mkt_share": x[0].mkt_share}): True})
    
    results = orders_lineitem_part_customer_nation_region_supplier_nation_7.sum(lambda x: {record({"mkt_share": x[0].mkt_share}): True})
    
    # Complete

    return results
