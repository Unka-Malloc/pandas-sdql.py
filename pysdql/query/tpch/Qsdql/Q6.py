from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):

    # Insert
    li_aggr = li.sum(lambda x_li: (((x_li[0].l_extendedprice) * (x_li[0].l_discount))) if (((((((((x_li[0].l_shipdate >= 19940101) * (x_li[0].l_shipdate < 19950101))) * (x_li[0].l_discount >= 0.049999999999999996))) * (x_li[0].l_discount <= 0.06999999999999999))) * (x_li[0].l_quantity < 24))) else (0.0))
    
    results = {record({"revenue": li_aggr}): True}
    # Complete

    return results
