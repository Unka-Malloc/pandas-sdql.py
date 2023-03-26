from pysdql.query.tpch.const import (LINEITEM_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "su": SUPPLIER_TYPE})
def query(li, su):

    # Insert
    df_rename_1_0 = df_rename_1.sum(lambda x: {x[0].concat(record({"total_revenue": x[0].total_revenue})): x[1]})
    
    supplier_df_rename_1_probe = df_rename_1_0.sum(lambda x: {x[0].concat(record({"supplier_no": x[0].supplier_no})): x[1]})
    
    supplier_df_rename_1_build_nest_dict = su.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    results = supplier_df_rename_1_probe.sum(lambda x: (supplier_df_rename_1_build_nest_dict[x[0].supplier_no].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_df_rename_1_build_nest_dict[x[0].supplier_no] != None) else (None))
    
    # Complete

    return results
