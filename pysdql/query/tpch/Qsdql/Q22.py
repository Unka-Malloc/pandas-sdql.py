from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(cu, ord):

    # Insert
    v13 = "13"
    v31 = "31"
    v23 = "23"
    v29 = "29"
    v30 = "30"
    v18 = "18"
    v17 = "17"
    customer_0 = cu.sum(lambda x: ({x[0]: x[1]}) if (((x[0].c_acctbal > ((x[0].sum_acctbal) / (x[0].count_acctbal))) * (((((((((((((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone, v30)))) + (startsWith(x[0].c_phone, v18)))) + (startsWith(x[0].c_phone, v17)))))) else (None))
    
    orders_customer_isin_build_index = ord.sum(lambda x: {x[0].o_custkey: True})
    
    customer_1 = customer_0.sum(lambda x: ({x[0]: x[1]}) if (orders_customer_isin_build_index[x[0].c_custkey] == None) else (None))
    
    customer_2 = customer_1.sum(lambda x: {x[0].concat(record({"cntrycode": substr(x[0].c_phone, 0, 1)})): x[1]})
    
    customer_3 = customer_2.sum(lambda x: {record({"cntrycode": x[0].cntrycode}): record({"numcust": (1.0) if (x[0].c_acctbal != None) else (0.0), "totacctbal": x[0].c_acctbal})})
    
    results = customer_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
