from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    lineitem_part_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None))
    
    lineitem_part_build_nest_dict = lineitem_part_build_pre_ops.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_0 = pa.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_part_1 = lineitem_part_0.sum(lambda x: {x[0].concat(record({"case_a": ((((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))) if (True) else (0)) if (startsWith(x[0].p_type, promo)) else (0)})): x[1]})
    
    lineitem_part_2 = lineitem_part_1.sum(lambda x: record({"case_a": x[0].case_a}))
    
    lineitem_part_3 = {lineitem_part_2: True}
    sumcase_a_el_0_case_a_sum = lineitem_part_3.sum(lambda x: ((x[0].l_extendedprice) * (((1) - (x[0].l_discount)))))
    
    suml_extendedprice1l_discount_el_0_JQ_l_extendedprice_mul_JQ_1_sub_l_discount_XZ_XZ = lineitem_part_3.sum(lambda x: ((x[0].l_extendedprice) * (((1) - (x[0].l_discount)))))
    
    df_aggr_1_attach_to_df_aggr_2 = df_aggr_1_1.sum(lambda x: {x[0].concat(record({"promo_revenue": ((((100.0) * (x[0].sumcase_a))) / (x[0].suml_extendedprice1l_discount))})): x[1]})
    
    results = df_aggr_1_attach_to_df_aggr_2.sum(lambda x: {record({"promo_revenue": x[0].promo_revenue}): True})
    
    # Complete

    return results
