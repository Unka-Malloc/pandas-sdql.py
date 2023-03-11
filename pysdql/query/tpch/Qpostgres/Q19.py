from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    lineitem_part_aggr = lineitem_part.sum(lambda x_lineitem_part: (((x_lineitem[0].l_extendedprice) * (((1) - (x_lineitem[0].l_discount))))) if (((((((((((((x_part[0].p_brand == brand12) * (((((((x_part[0].p_container == smpkg) + (x_part[0].p_container == smpack))) + (x_part[0].p_container == smcase))) + (x_part[0].p_container == smbox))))) * (x_lineitem[0].l_quantity >= 1))) * (x_lineitem[0].l_quantity <= 11))) * (x_part[0].p_size <= 5))) + (((((((((x_part[0].p_brand == brand23) * (((((((x_part[0].p_container == medpack) + (x_part[0].p_container == medpkg))) + (x_part[0].p_container == medbag))) + (x_part[0].p_container == medbox))))) * (x_lineitem[0].l_quantity >= 10))) * (x_lineitem[0].l_quantity <= 20))) * (x_part[0].p_size <= 10))))) + (((((((((x_part[0].p_brand == brand34) * (((((((x_part[0].p_container == lgpkg) + (x_part[0].p_container == lgpack))) + (x_part[0].p_container == lgcase))) + (x_part[0].p_container == lgbox))))) * (x_lineitem[0].l_quantity >= 20))) * (x_lineitem[0].l_quantity <= 30))) * (x_part[0].p_size <= 15))))) else (0))
    
    df_aggr_1 = {record({"revenue": lineitem_part_aggr}): True}
    results = df_aggr_1
    # Complete

    return results
