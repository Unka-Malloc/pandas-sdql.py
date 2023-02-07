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
    cu1_aggr = cu1.sum(lambda x_cu1: (record({"sum_acctbal": x_cu1[0].c_acctbal, "count_acctbal": 1})) if (((x_cu1[0].c_acctbal > 0.0) * (((((((((((((startsWith(x_cu1[0].c_phone, v13)) + (startsWith(x_cu1[0].c_phone, v31)))) + (startsWith(x_cu1[0].c_phone, v23)))) + (startsWith(x_cu1[0].c_phone, v29)))) + (startsWith(x_cu1[0].c_phone, v30)))) + (startsWith(x_cu1[0].c_phone, v18)))) + (startsWith(x_cu1[0].c_phone, v17)))))) else (None))
    
    orders_part = ord.sum(lambda x_orders: {x_orders[0].o_custkey: True})
    
    customer_aggr = cu.sum(lambda x_customer: (({substr(x_customer[0].c_phone, 0, 1): record({"numcust": 1, "totalacctbal": x_customer[0].c_acctbal})}) if (orders_part[x_customer[0].c_custkey] == None) else (None)) if (((x_customer[0].c_acctbal > ((cu1_aggr.sum_acctbal) / (cu1_aggr.count_acctbal))) * (((((((((((((startsWith(x_customer[0].c_phone, v13)) + (startsWith(x_customer[0].c_phone, v31)))) + (startsWith(x_customer[0].c_phone, v23)))) + (startsWith(x_customer[0].c_phone, v29)))) + (startsWith(x_customer[0].c_phone, v30)))) + (startsWith(x_customer[0].c_phone, v18)))) + (startsWith(x_customer[0].c_phone, v17)))))) else (None))
    
    results = customer_aggr.sum(lambda x_customer_aggr: {record({"cntrycode": x_customer_aggr[0], "numcust": x_customer_aggr[1].numcust, "totalacctbal": x_customer_aggr[1].totalacctbal}): True})
    
    # Complete

    return results
