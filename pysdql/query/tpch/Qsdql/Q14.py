from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):
    # Insert
    promo = "PROMO"
    part_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None))
    
    part_lineitem_build_nest_dict = pa.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    part_lineitem_0 = part_lineitem_probe_pre_ops.sum(lambda x: (part_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (part_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))
    
    part_lineitem_1 = part_lineitem_0.sum(lambda x: {x[0].concat(record({"A": (((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) if (startsWith(x[0].p_type, promo)) else (0.0)})): x[1]})
    
    part_lineitem_2 = part_lineitem_1.sum(lambda x: {x[0].concat(record({"B": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    JQ_JQ_A_mul_1000_XZ_div_B_XZ_pre_ops = part_lineitem_2.sum(lambda x: record({"A_sum": x[0].A, "B_sum": x[0].B}))
    
    results = ((((JQ_JQ_A_mul_1000_XZ_div_B_XZ_pre_ops.A_sum) * (100.0))) / (JQ_JQ_A_mul_1000_XZ_div_B_XZ_pre_ops.B_sum))
    # Complete

    return results
