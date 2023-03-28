from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, 'l1': LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, l1, pa):

    # Insert
    brand23 = "Brand#23"
    medbox = "MED BOX"
    lineitem_part_probe_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (((x[0].p_brand == brand23) * (x[0].p_container == medbox))) else (None))
    
    lineitem_part_build_nest_dict = li.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_lineitem_part_build_pre_ops = lineitem_part_probe_pre_ops.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    lineitem_part_build_pre_ops = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey, "l_linenumber": x[0].l_linenumber, "l_quantity": x[0].l_quantity, "l_extendedprice": x[0].l_extendedprice, "l_discount": x[0].l_discount, "l_tax": x[0].l_tax, "l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "l_shipdate": x[0].l_shipdate, "l_commitdate": x[0].l_commitdate, "l_receiptdate": x[0].l_receiptdate, "l_shipinstruct": x[0].l_shipinstruct, "l_shipmode": x[0].l_shipmode, "l_comment": x[0].l_comment}): True})
    
    lineitem_part_build_nest_dict = lineitem_part_build_pre_ops.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_0 = pa.sum(lambda x: (lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_part_1 = lineitem_part_0.sum(lambda x: {record({"p_partkey": x[0].p_partkey}): record({"mean_l_quantity_sum_for_mean": x[0].l_quantity, "mean_l_quantity_count_for_mean": 1.0})})
    
    lineitem_part_2 = lineitem_part_1.sum(lambda x: {record({"p_partkey": x[0].p_partkey, "mean_l_quantity": ((x[1].mean_l_quantity_sum_for_mean) / (x[1].mean_l_quantity_count_for_mean))}): True})
    
    lineitem_part_3 = lineitem_part_2.sum(lambda x: {x[0].concat(record({"avgl_quantity": ((0.2) * (x[0].mean_l_quantity))})): x[1]})
    
    lineitem_part_4 = lineitem_part_3.sum(lambda x: {record({"avgl_quantity": x[0].avgl_quantity, "p_partkey": x[0].p_partkey}): True})
    
    lineitem_part_lineitem_part_probe_pre_ops = lineitem_part_4.sum(lambda x: {x[0]: x[1]})
    
    lineitem_part_lineitem_part_build_nest_dict = lineitem_part_lineitem_part_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})
    
    lineitem_part_lineitem_part_0 = lineitem_part_lineitem_part_probe_pre_ops.sum(lambda x: (lineitem_part_lineitem_part_build_nest_dict[x[0].p_partkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (lineitem_part_lineitem_part_build_nest_dict[x[0].p_partkey] != None) else (None))
    
    lineitem_part_lineitem_part_1 = lineitem_part_lineitem_part_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_quantity < x[0].avgl_quantity) else (None))
    
    lineitem_part_lineitem_part_2 = lineitem_part_lineitem_part_1.sum(lambda x: record({"l_extendedprice_sum": x[0].l_extendedprice}))
    
    results = ((lineitem_part_lineitem_part_2.l_extendedprice_sum) / (7.0))
    # Complete

    return results
