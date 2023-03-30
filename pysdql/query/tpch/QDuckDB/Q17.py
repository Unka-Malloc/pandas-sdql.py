from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, 'l1': LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, l1, pa):

    # Insert
    brand23 = "Brand#23"
    medbox = "MED BOX"
    lineitem_part_probe_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (((x[0].p_brand == brand23) * (x[0].p_container == medbox))) else (None))
    
    lineitem_part_build_nest_dict = li.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_lineitem_build_pre_ops = lineitem_part_probe_pre_ops.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_0 = li.sum(lambda x: {record({"l_partkey": x[0].l_partkey}): record({"mean_l_quantity_sum_for_mean": x[0].l_quantity, "mean_l_quantity_count_for_mean": 1.0})})
    
    lineitem_1 = lineitem_0.sum(lambda x: {record({"l_partkey": x[0].l_partkey, "mean_l_quantity": ((x[1].mean_l_quantity_sum_for_mean) / (x[1].mean_l_quantity_count_for_mean))}): True})
    
    lineitem_2 = lineitem_1.sum(lambda x: {x[0].concat(record({"avgl_quantity": x[0].mean_l_quantity})): x[1]})
    
    lineitem_part_lineitem_probe_pre_ops = lineitem_2.sum(lambda x: {x[0]: x[1]})
    
    lineitem_part_lineitem_build_nest_dict = lineitem_part_lineitem_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_lineitem_0 = lineitem_part_lineitem_probe_pre_ops.sum(lambda x: (lineitem_part_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))
    
    lineitem_part_lineitem_1 = lineitem_part_lineitem_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_quantity < ((0.2) * (x[0].avgl_quantity))) else (None))
    
    lineitem_part_lineitem_2 = lineitem_part_lineitem_1.sum(lambda x: record({"l_extendedprice": x[0].l_extendedprice}))
    
    suml_extendedprice_el_0_l_extendedprice_sum = sr_dict({lineitem_part_lineitem_2: True})
    df_aggr_1_attach_to_df_aggr_2 = df_aggr_1_0.sum(lambda x: {x[0].concat(record({"avg_yearly": ((x[0].suml_extendedprice) / (7.0))})): x[1]})
    
    results = df_aggr_1_attach_to_df_aggr_2.sum(lambda x: {record({"avg_yearly": x[0].avg_yearly}): True})
    
    # Complete

    return results
