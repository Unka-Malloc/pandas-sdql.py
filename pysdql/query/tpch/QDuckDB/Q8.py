from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE,
                                     NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE,
               "na": NATION_TYPE, "n1": NATION_TYPE, "n2": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, li, ord, cu, na, n1, n2, re):

    # Insert
    orders_customer_nation_region_build_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19950101) * (x[0].o_orderdate <= 19961231))) else (None))
    
    nation_region_probe_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == america) else (None))
    
    nation_region_build_nest_dict = na.sum(lambda x: {x[0].n_regionkey: sr_dict({x[0]: x[1]})})
    
    customer_nation_region_probe_pre_ops = nation_region_probe_pre_ops.sum(lambda x: (nation_region_build_nest_dict[x[0].r_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_region_build_nest_dict[x[0].r_regionkey] != None) else (None))
    
    customer_nation_region_build_nest_dict = cu.sum(lambda x: {x[0].c_nationkey: sr_dict({x[0]: x[1]})})
    
    orders_customer_nation_region_probe_pre_ops = customer_nation_region_probe_pre_ops.sum(lambda x: (customer_nation_region_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (customer_nation_region_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    orders_customer_nation_region_build_nest_dict = orders_customer_nation_region_build_pre_ops.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_region_probe_pre_ops = orders_customer_nation_region_probe_pre_ops.sum(lambda x: (orders_customer_nation_region_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (orders_customer_nation_region_build_nest_dict[x[0].c_custkey] != None) else (None))
    
    lineitem_orders_customer_nation_region_build_nest_dict = li.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_region_supplier_nation_build_pre_ops = lineitem_orders_customer_nation_region_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_nation_region_build_nest_dict[x[0].o_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_nation_region_build_nest_dict[x[0].o_orderkey] != None) else (None))
    
    supplier_nation_build_nest_dict = su.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_region_supplier_nation_probe_pre_ops = na.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    lineitem_orders_customer_nation_region_supplier_nation_build_nest_dict = lineitem_orders_customer_nation_region_supplier_nation_build_pre_ops.sum(lambda x: {x[0].l_suppkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_build_pre_ops = lineitem_orders_customer_nation_region_supplier_nation_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_nation_region_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_nation_region_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    lineitem_orders_customer_nation_region_supplier_nation_part_probe_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (x[0].p_type == economyanodizedsteel) else (None))
    
    lineitem_orders_customer_nation_region_supplier_nation_part_build_nest_dict = lineitem_orders_customer_nation_region_supplier_nation_part_build_pre_ops.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_0 = lineitem_orders_customer_nation_region_supplier_nation_part_probe_pre_ops.sum(lambda x: (lineitem_orders_customer_nation_region_supplier_nation_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_orders_customer_nation_region_supplier_nation_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_orders_customer_nation_region_supplier_nation_part_1 = lineitem_orders_customer_nation_region_supplier_nation_part_0.sum(lambda x: {x[0].concat(record({"case_a": (((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))) if (x[0].n_name == brazil) else (0)})): x[1]})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_2 = lineitem_orders_customer_nation_region_supplier_nation_part_1.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_3 = lineitem_orders_customer_nation_region_supplier_nation_part_2.sum(lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_4 = lineitem_orders_customer_nation_region_supplier_nation_part_3.sum(lambda x: {record({"o_year": x[0].o_year}): record({"sum_case_a": x[0].case_a, "sum_volume": x[0].volume})})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_5 = lineitem_orders_customer_nation_region_supplier_nation_part_4.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_6 = lineitem_orders_customer_nation_region_supplier_nation_part_5.sum(lambda x: {x[0].concat(record({"sumcase_a": x[0].sum_case_a})): x[1]})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_7 = lineitem_orders_customer_nation_region_supplier_nation_part_6.sum(lambda x: {x[0].concat(record({"suml_extendedprice1l_discount": x[0].sum_volume})): x[1]})
    
    lineitem_orders_customer_nation_region_supplier_nation_part_attach_to_df_aggr_1 = lineitem_orders_customer_nation_region_supplier_nation_part_7.sum(lambda x: {x[0]: x[1]})
    
    df_aggr_1_0 = df_aggr_1.sum(lambda x: {x[0].concat(record({"mkt_share": ((x[0].sumcase_a) / (x[0].suml_extendedprice1l_discount))})): x[1]})
    
    results = lineitem_orders_customer_nation_region_supplier_nation_part_attach_to_df_aggr_1.sum(lambda x: {record({"o_year": x[0].o_year}): True})
    
    # Complete

    return results
