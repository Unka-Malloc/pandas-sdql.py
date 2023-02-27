from pysdql.extlib.sdqlpy.sdql_lib import *
@sdql_compile({})
def query():
    li_aggr = li.sum(lambda x_li: ((x_li[0].l_extendedprice) * (x_li[0].l_discount)))
    
    df_aggr_1 = {record({"revenue": li_aggr}): True}
    results = df_aggr_1
    return results
