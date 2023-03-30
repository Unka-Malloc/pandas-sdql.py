from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    df_aggr_1_0 = df_aggr_1.sum(lambda x: {x[0].concat(record({"l_extendedprice1l_discount": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    df_aggr_1_1 = df_aggr_1_0.sum(lambda x: record({"l_extendedprice1l_discount": x[0].l_extendedprice1l_discount}))
    
    results = {df_aggr_1_1: True}
    # Complete

    return results
