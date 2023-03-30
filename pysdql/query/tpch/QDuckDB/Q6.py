from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):

    # Insert
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) * (x[0].l_discount >= 0.05))) * (x[0].l_discount <= 0.07))) * (x[0].l_quantity < 24))) else (None))
    
    lineitem_attach_to_df_aggr_1 = lineitem_0.sum(lambda x: {x[0].concat(record({"l_extendedpricel_discount": ((x[0].l_extendedprice) * (x[0].l_discount))})): x[1]})
    
    df_aggr_1_0 = lineitem_attach_to_df_aggr_1.sum(lambda x: {record({"l_extendedpricel_discount": x[0].l_extendedpricel_discount}): True})
    
    df_aggr_1_1 = df_aggr_1_0.sum(lambda x: record({"l_extendedpricel_discount": x[0].l_extendedpricel_discount}))
    
    results = {record({"revenue": df_aggr_1_1.l_extendedpricel_discount}): True}
    # Complete

    return results
