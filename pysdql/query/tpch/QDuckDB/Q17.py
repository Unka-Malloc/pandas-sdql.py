from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, 'l1': LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, l1, pa):

    # Insert
    df_aggr_2_0 = df_aggr_2.sum(lambda x: {x[0].concat(record({"avg_yearly": ((x[0].suml_extendedprice) / (7.0))})): x[1]})
    
    results = df_aggr_2_0.sum(lambda x: {record({"avg_yearly": x[0].avg_yearly}): True})
    
    # Complete

    return results
