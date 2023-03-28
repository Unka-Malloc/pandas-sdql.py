from pysdql.query.tpch.const import (LINEITEM_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "su": SUPPLIER_TYPE})
def query(li, su):

    # Insert
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19960101) * (x[0].l_shipdate < 19960401))) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    lineitem_1 = lineitem_0.sum(lambda x: {x[0].concat(record({"supplier_no": x[0].l_suppkey})): x[1]})
    
    lineitem_2 = lineitem_1.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].l_extendedprice) * (((1) - (x[0].l_discount))))})): x[1]})
    
    lineitem_3 = lineitem_2.sum(lambda x: {record({"supplier_no": x[0].supplier_no}): record({"total_revenue": x[0].before_1, "sum_before_1": x[0].before_1})})
    
    lineitem_4 = lineitem_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    lineitem_5 = lineitem_4.sum(lambda x: {x[0].concat(record({"suml_extendedprice1l_discount": x[0].sum_before_1})): x[1]})
    
    lineitem_6 = lineitem_5.sum(lambda x: ({x[0]: x[1]}) if (x[0].suml_extendedprice1l_discount == 1614410.2928000002) else (None))
    
    lineitem_7 = lineitem_6.sum(lambda x: {record({"total_revenue": x[0].total_revenue}): True})
    
    lineitem_attach_to_df_rename_1 = lineitem_6.sum(lambda x: {x[0]: x[1]})
    
    supplier_df_rename_1_probe_pre_ops = lineitem_attach_to_df_rename_1.sum(lambda x: {record({"total_revenue": x[0].total_revenue, "supplier_no": x[0].supplier_no}): True})
    
    supplier_df_rename_1_build_nest_dict = su.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    supplier_df_rename_1_0 = supplier_df_rename_1_probe_pre_ops.sum(lambda x: (supplier_df_rename_1_build_nest_dict[x[0].supplier_no].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_df_rename_1_build_nest_dict[x[0].supplier_no] != None) else (None))
    
    results = supplier_df_rename_1_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_name": x[0].s_name, "s_address": x[0].s_address, "s_phone": x[0].s_phone, "total_revenue": x[0].total_revenue}): True})
    
    # Complete

    return results
