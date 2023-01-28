from pysdql.query.tpch.const import (ORDERS_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"ord": ORDERS_TYPE, "li": LINEITEM_TYPE})
def query(ord, li):

    # Insert

    li_part = li.sum(
        lambda x_li: ({x_li[0].l_orderkey: True}) if (x_li[0].l_commitdate < x_li[0].l_receiptdate) else (None))

    ord_groupby_agg = ord.sum(
        lambda x_ord: (({x_ord[0].o_orderpriority: 1}) if (li_part[x_ord[0].o_orderkey] != None) else (None)) if (
        ((x_ord[0].o_orderdate >= 19930701) * (x_ord[0].o_orderdate < 19931001))) else (None))

    results = ord_groupby_agg.sum(lambda x_ord_groupby_agg: {
        record({"o_orderpriority": x_ord_groupby_agg[0], "order_count": x_ord_groupby_agg[1]}): True})

    # Complete

    return results
