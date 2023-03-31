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
    orders_0 = ord.sum(lambda x: {x[0]: {record({"o_custkey": x[0].o_custkey}): True}})
    
    orders_customer_build_pre_ops = orders_0.sum(lambda x: x[1])
    
    customer_0 = cu.sum(lambda x: ({x[0]: x[1]}) if (((x[0].c_acctbal > 0.0) * (((((((((((((substr(x[0].c_phone, 0, 1) == v17) + (substr(x[0].c_phone, 0, 1) == v18))) + (substr(x[0].c_phone, 0, 1) == v13))) + (substr(x[0].c_phone, 0, 1) == v31))) + (substr(x[0].c_phone, 0, 1) == v23))) + (substr(x[0].c_phone, 0, 1) == v29))) + (substr(x[0].c_phone, 0, 1) == v30))))) else (None))
    
    customer_1 = customer_0.sum(lambda x: (record({"c_acctbal_sum_for_mean": x[0].c_acctbal, "c_acctbal_count_for_mean": 1.0})) if (x[0].c_acctbal != None) else (0.0))
    
    avgc_acctbal_el_0_c_acctbal_mean = ((customer_1.c_acctbal_sum_for_mean) / (customer_1.c_acctbal_count_for_mean))
    orders_customer_probe_pre_ops = cu.sum(lambda x: ({x[0]: x[1]}) if (((x[0].c_acctbal > avgc_acctbal_el_0_c_acctbal_mean) * (((((((((((((substr(x[0].c_phone, 0, 1) == v17) + (substr(x[0].c_phone, 0, 1) == v18))) + (substr(x[0].c_phone, 0, 1) == v13))) + (substr(x[0].c_phone, 0, 1) == v31))) + (substr(x[0].c_phone, 0, 1) == v23))) + (substr(x[0].c_phone, 0, 1) == v29))) + (substr(x[0].c_phone, 0, 1) == v30))))) else (None))
    
    customer_orders_build_nest_dict = orders_customer_build_pre_ops.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})
    
    customer_orders_0 = orders_customer_probe_pre_ops.sum(lambda x: ({x[0]: True}) if (customer_orders_build_nest_dict[x[0].c_custkey] == None) else (None))
    
    customer_orders_1 = customer_orders_0.sum(lambda x: {x[0].concat(record({"cntrycode": substr(x[0].c_phone, 0, 1)})): x[1]})
    
    customer_orders_2 = customer_orders_1.sum(lambda x: {record({"cntrycode": x[0].cntrycode}): record({"numcust": (1.0) if (x[0].cntrycode != None) else (0.0), "totacctbal": x[0].c_acctbal})})
    
    customer_orders_3 = customer_orders_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    customer_orders_4 = customer_orders_3.sum(lambda x: {x[0]: {record({"numcust": x[0].numcust, "totacctbal": x[0].totacctbal}): True}})
    
    results = customer_orders_4.sum(lambda x: x[1])
    
    # Complete

    return results
