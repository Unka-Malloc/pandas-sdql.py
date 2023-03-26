from pysdql.query.tpch.const import (LINEITEM_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "su": SUPPLIER_TYPE})
def query(li, su):

    # Insert
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19960101) * (x[0].l_shipdate < 19960401))) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    lineitem_2 = lineitem_1.sum(lambda x: {record({"l_suppkey": x[0].l_suppkey}): record({"total_revenue": x[0].revenue})})
    
    lineitem_3 = lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    supplier_lineitem_probe_pre_ops = lineitem_3.sum(lambda x: ({x[0]: x[1]}) if (x[0].total_revenue == 1772627.2087) else (None))
    
    supplier_lineitem_build_nest_dict = su.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    supplier_lineitem_0 = supplier_lineitem_probe_pre_ops.sum(lambda x: (supplier_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))
    
    results = supplier_lineitem_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_name": x[0].s_name, "s_address": x[0].s_address, "s_phone": x[0].s_phone, "total_revenue": x[0].total_revenue}): True})
    
    # Complete

    return results
