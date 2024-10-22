from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):
    
    # Insert
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate <= 19980902) else (None))
    
    lineitem_1 = lineitem_0.sum(lambda x: {x[0].concat(record({"disc_price": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})
    
    lineitem_2 = lineitem_1.sum(lambda x: {x[0].concat(record({"charge": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) * (((1.0) + (x[0].l_tax))))})): x[1]})
    
    lineitem_3 = lineitem_2.sum(lambda x: {record({"l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus}): record({"sum_qty": x[0].l_quantity, "sum_base_price": x[0].l_extendedprice, "sum_disc_price": x[0].disc_price, "sum_charge": x[0].charge, "avg_qty_sum_for_mean": x[0].l_quantity, "avg_qty_count_for_mean": 1.0, "avg_price_sum_for_mean": x[0].l_extendedprice, "avg_price_count_for_mean": 1.0, "avg_disc_sum_for_mean": x[0].l_discount, "avg_disc_count_for_mean": 1.0, "count_order": (1.0) if (x[0].l_quantity != None) else (0.0)})})
    
    results = lineitem_3.sum(lambda x: {record({"l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "sum_qty": x[1].sum_qty, "sum_base_price": x[1].sum_base_price, "sum_disc_price": x[1].sum_disc_price, "sum_charge": x[1].sum_charge, "avg_qty": ((x[1].avg_qty_sum_for_mean) / (x[1].avg_qty_count_for_mean)), "avg_price": ((x[1].avg_price_sum_for_mean) / (x[1].avg_price_count_for_mean)), "avg_disc": ((x[1].avg_disc_sum_for_mean) / (x[1].avg_disc_count_for_mean)), "count_order": x[1].count_order}): True})
    
    # Complete

    return results
