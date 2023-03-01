from pysdql.query.tpch.const import (LINEITEM_TYPE, SUPPLIER_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "su": SUPPLIER_TYPE})
def query(li, su):

    # Insert
    lineitem_aggr = li.sum(lambda x_lineitem: ({x_lineitem[0].l_suppkey: ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))}) if (((x_lineitem[0].l_shipdate >= 19960101) * (x_lineitem[0].l_shipdate < 19960401))) else (None))
    
    supplier_part = su.sum(lambda x_supplier: {x_supplier[0].s_suppkey: record({"s_suppkey": x_supplier[0].s_suppkey, "s_name": x_supplier[0].s_name, "s_address": x_supplier[0].s_address, "s_phone": x_supplier[0].s_phone})})
    
    results = lineitem_aggr.sum(lambda x_lineitem_aggr: (({record({"s_suppkey": x_lineitem_aggr[0], "s_name": supplier_part[x_lineitem_aggr[0]].s_name, "s_address": supplier_part[x_lineitem_aggr[0]].s_address, "s_phone": supplier_part[x_lineitem_aggr[0]].s_phone, "total_revenue": x_lineitem_aggr[1]}): True}) if (x_lineitem_aggr[1] == 797313.3838) else (None)) if (supplier_part[x_lineitem_aggr[0]] != None) else (None))
    
    # Complete

    return results
