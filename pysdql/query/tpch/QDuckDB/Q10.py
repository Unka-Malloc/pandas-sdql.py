from pysdql.query.tpch.const import (CUSTOMER_TYPE, ORDERS_TYPE, LINEITEM_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE, "li": LINEITEM_TYPE, "na": NATION_TYPE})
def query(cu, ord, li, na):

    # Insert
    df_aggr_1_0 = df_aggr_1.sum(lambda x: {x[0].concat(record({"c_custkey": x[0].c_custkey})): x[1]})
    
    df_aggr_1_1 = df_aggr_1_0.sum(lambda x: {x[0].concat(record({"c_name": x[0].c_name})): x[1]})
    
    df_aggr_1_2 = df_aggr_1_1.sum(lambda x: {x[0].concat(record({"revenue": x[0].revenue})): x[1]})
    
    df_aggr_1_3 = df_aggr_1_2.sum(lambda x: {x[0].concat(record({"c_acctbal": x[0].c_acctbal})): x[1]})
    
    df_aggr_1_4 = df_aggr_1_3.sum(lambda x: {x[0].concat(record({"n_name": x[0].n_name})): x[1]})
    
    df_aggr_1_5 = df_aggr_1_4.sum(lambda x: {x[0].concat(record({"c_address": x[0].c_address})): x[1]})
    
    df_aggr_1_6 = df_aggr_1_5.sum(lambda x: {x[0].concat(record({"c_phone": x[0].c_phone})): x[1]})
    
    results = df_aggr_1_6.sum(lambda x: {x[0].concat(record({"c_comment": x[0].c_comment})): x[1]})
    
    # Complete

    return results
