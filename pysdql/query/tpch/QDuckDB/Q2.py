from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "ps1": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, ps1, na, re):

    # Insert
    df_aggr_1_0 = df_aggr_1.sum(lambda x: {x[0].concat(record({"s_acctbal": x[0].s_acctbal})): x[1]})
    
    df_aggr_1_1 = df_aggr_1_0.sum(lambda x: {x[0].concat(record({"s_name": x[0].s_name})): x[1]})
    
    df_aggr_1_2 = df_aggr_1_1.sum(lambda x: {x[0].concat(record({"n_name": x[0].n_name})): x[1]})
    
    df_aggr_1_3 = df_aggr_1_2.sum(lambda x: {x[0].concat(record({"p_partkey": x[0].p_partkey})): x[1]})
    
    df_aggr_1_4 = df_aggr_1_3.sum(lambda x: {x[0].concat(record({"p_mfgr": x[0].p_mfgr})): x[1]})
    
    df_aggr_1_5 = df_aggr_1_4.sum(lambda x: {x[0].concat(record({"s_address": x[0].s_address})): x[1]})
    
    df_aggr_1_6 = df_aggr_1_5.sum(lambda x: {x[0].concat(record({"s_phone": x[0].s_phone})): x[1]})
    
    results = df_aggr_1_6.sum(lambda x: {x[0].concat(record({"s_comment": x[0].s_comment})): x[1]})
    
    # Complete

    return results
