from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):
    
    # Insert
    lineitem_aggr = li.sum(lambda x_lineitem: ({record({"l_returnflag": x_lineitem[0].l_returnflag, "l_linestatus": x_lineitem[0].l_linestatus}): record({"sum_qty": x_lineitem[0].l_quantity, "sum_base_price": x_lineitem[0].l_extendedprice, "sum_disc_price": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount)))), "sum_charge": ((((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) * (((1.0) + (x_lineitem[0].l_tax)))), "avg_disc_sum_for_mean": x_lineitem[0].l_discount, "count_order": 1.0})}) if (x_lineitem[0].l_shipdate <= 19980902) else (None))
    
    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record({"l_returnflag": x_lineitem_aggr[0].l_returnflag, "l_linestatus": x_lineitem_aggr[0].l_linestatus, "sum_qty": x_lineitem_aggr[1].sum_qty, "sum_base_price": x_lineitem_aggr[1].sum_base_price, "sum_disc_price": x_lineitem_aggr[1].sum_disc_price, "sum_charge": x_lineitem_aggr[1].sum_charge, "avg_qty": ((x_lineitem_aggr[1].sum_qty) / (x_lineitem_aggr[1].count_order)), "avg_price": ((x_lineitem_aggr[1].sum_base_price) / (x_lineitem_aggr[1].count_order)), "avg_disc": ((x_lineitem_aggr[1].avg_disc_sum_for_mean) / (x_lineitem_aggr[1].count_order)), "count_order": x_lineitem_aggr[1].count_order}): True})
    
    # Complete

    return results
