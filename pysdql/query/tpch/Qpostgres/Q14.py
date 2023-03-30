from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    promo = "PROMO"
    lineitem_part_build_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None))
    
    lineitem_part_build_nest_dict = lineitem_part_build_pre_ops.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_0 = pa.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_part_1 = lineitem_part_0.sum(lambda x: {x[0].concat(record({"case_a": ((((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))) if (True) else (0)) if (startsWith(x[0].p_type, promo)) else (0)})): x[1]})
    
    JQ_JQ_1000_mul_case_a_XZ_div_JQ_l_extendedprice_mul_JQ_1_sub_l_discount_XZ_XZ_XZ_pre_ops = lineitem_part_1.sum(lambda x: record({"case_a_sum": x[0].case_a, "JQ_l_extendedprice_mul_JQ_1_sub_l_discount_XZ_XZ": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))}))
    
    results = ((((100.0) * (JQ_JQ_1000_mul_case_a_XZ_div_JQ_l_extendedprice_mul_JQ_1_sub_l_discount_XZ_XZ_XZ_pre_ops.case_a_sum))) / (JQ_JQ_1000_mul_case_a_XZ_div_JQ_l_extendedprice_mul_JQ_1_sub_l_discount_XZ_XZ_XZ_pre_ops.JQ_l_extendedprice_mul_JQ_1_sub_l_discount_XZ_XZ))
    # Complete

    return results
