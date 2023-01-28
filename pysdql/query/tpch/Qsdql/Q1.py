from pysdql.query.tpch.const import (LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE})
def query(li):
    # Insert

    li_groupby_agg = li.sum(lambda x_li: ({record({"l_returnflag": x_li[0].l_returnflag, "l_linestatus": x_li[0].l_linestatus}): record({"sum_qty": x_li[0].l_quantity, "sum_base_price": x_li[0].l_extendedprice, "sum_disc_price": ((x_li[0].l_extendedprice) * (((1.0) - (x_li[0].l_discount)))), "sum_charge": ((((x_li[0].l_extendedprice) * (((1.0) - (x_li[0].l_discount))))) * (((1.0) + (x_li[0].l_tax)))), "count_order": 1})}) if (x_li[0].l_shipdate <= 19980902) else (None))

    results = li_groupby_agg.sum(lambda x_li_groupby_agg: {x_li_groupby_agg[0].concat(x_li_groupby_agg[1]): True})

    # Complete

    return results
