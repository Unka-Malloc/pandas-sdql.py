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
    cu1_aggr = cu.sum(lambda x_customer: (record({"sum_acctbal": x_customer[0].c_acctbal, "count_acctbal": 1.0})) if (((x_customer[0].c_acctbal > 0.0) * (((((((((((((startsWith(x_customer[0].c_phone, v13)) + (startsWith(x_customer[0].c_phone, v31)))) + (startsWith(x_customer[0].c_phone, v23)))) + (startsWith(x_customer[0].c_phone, v29)))) + (startsWith(x_customer[0].c_phone, v30)))) + (startsWith(x_customer[0].c_phone, v18)))) + (startsWith(x_customer[0].c_phone, v17)))))) else (None))
    
    count_acctbal = cu1_aggr.count_acctbal
    sum_acctbal = cu1_aggr.sum_acctbal
    orders_part = ord.sum(lambda x_orders: {x_orders[0].o_custkey: True})
    
    customer_aggr = cu.sum(lambda x_customer: (({substr(x_customer[0].c_phone, 0, 1): record({"numcust": 1.0, "totacctbal": x_customer[0].c_acctbal})}) if (orders_part[x_customer[0].c_custkey] == None) else (None)) if (((x_customer[0].c_acctbal > ((sum_acctbal) / (count_acctbal))) * (((((((((((((startsWith(x_customer[0].c_phone, v13)) + (startsWith(x_customer[0].c_phone, v31)))) + (startsWith(x_customer[0].c_phone, v23)))) + (startsWith(x_customer[0].c_phone, v29)))) + (startsWith(x_customer[0].c_phone, v30)))) + (startsWith(x_customer[0].c_phone, v18)))) + (startsWith(x_customer[0].c_phone, v17)))))) else (None))
    
    results = customer_aggr.sum(lambda x_customer_aggr: {record({"cntrycode": x_customer_aggr[0], "numcust": x_customer_aggr[1].numcust, "totacctbal": x_customer_aggr[1].totacctbal}): True})
    
    # Complete

    return results
