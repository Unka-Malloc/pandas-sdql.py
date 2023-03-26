from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE,
                                     NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE,
               "na": NATION_TYPE, "n1": NATION_TYPE, "n2": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, li, ord, cu, na, n1, n2, re):

    # Insert
    df_aggr_1_0 = df_aggr_1.sum(lambda x: {x[0].concat(record({"o_year": x[0].o_year})): x[1]})
    
    results = df_aggr_1_0.sum(lambda x: {x[0].concat(record({"mkt_share": ((x[0].sumcase_a) / (x[0].suml_extendedprice1l_discount))})): x[1]})
    
    # Complete

    return results
