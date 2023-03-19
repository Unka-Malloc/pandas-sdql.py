from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "cu1": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(cu, cu1, ord):

    # Insert
    v13 = "13"
    v31 = "31"
    v23 = "23"
    v29 = "29"
    v30 = "30"
    v18 = "18"
    v17 = "17"
    customer_orders_isin_build = ord
    v0 = cu1.sum(lambda x: (({x[0]: x[1]}) if (((x[0].c_acctbal > 0.0) * (((((((((((((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone, v30)))) + (startsWith(x[0].c_phone, v18)))) + (startsWith(x[0].c_phone, v17)))))) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: ((record({"sum_acctbal": x[0].c_acctbal, "count_acctbal": 1})) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({v1: True}) if (True) else (None)) if (x[0] != None) else (None))
    
    prev_aggregation = v1
    v0 = cu.sum(lambda x: (({x[0]: x[1]}) if (((x[0].c_acctbal > ((prev_aggregation.sum_acctbal) / (prev_aggregation.count_acctbal))) * (((((((((((((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone, v30)))) + (startsWith(x[0].c_phone, v18)))) + (startsWith(x[0].c_phone, v17)))))) else (None)) if (x[0] != None) else (None))
    
    customer_orders_isin_build = ord.sum(lambda x: (({x[0].o_custkey: True}) if (True) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: ((({x[0]: x[1]}) if (customer_orders_isin_build[x[0].c_custkey] == None) else (None)) if (True) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({x[0].concat(record({"cntrycode": substr(x[0].c_phone, 0, 1)})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({record({"cntrycode": x[0].cntrycode}): record({"numcust": 1, "totacctbal": x[0].c_acctbal})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v4 = v3.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v4
    # Complete

    return results
