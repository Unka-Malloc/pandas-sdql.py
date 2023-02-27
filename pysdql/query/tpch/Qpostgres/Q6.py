from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):

    # Insert
    lineitem_aggr = li.sum(lambda x_lineitem: (((x_lineitem[0].l_extendedprice) * (x_lineitem[0].l_discount))) if (((((((((x_lineitem[0].l_shipdate >= 1994010100000) * (x_lineitem[0].l_shipdate < 1995010100000))) * (x_lineitem[0].l_discount >= 0.05))) * (x_lineitem[0].l_discount <= 0.07))) * (x_lineitem[0].l_quantity < 24))) else (0))
    
    df_aggr_1 = {record({"revenue": lineitem_aggr}): True}
    results = df_aggr_1
    # Complete

    return results
