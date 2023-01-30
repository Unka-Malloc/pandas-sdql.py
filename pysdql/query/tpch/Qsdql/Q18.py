from pysdql.query.tpch.const import (LINEITEM_TYPE, CUSTOMER_TYPE, ORDERS_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "cu": CUSTOMER_TYPE, "ord": ORDERS_TYPE})
def query(li, cu, ord):

    # Insert

    li_groupby_agg = li.sum(lambda x_li: ({x_li[0].l_orderkey: x_li[0].l_quantity}) if (True) else (None))

    li_having = li_groupby_agg.sum(
        lambda x_li_groupby_agg: ({x_li_groupby_agg[0]: True}) if (x_li_groupby_agg[1] > 300) else (None))

    cu_part = cu.sum(lambda x_cu: {x_cu[0].c_custkey: record({"c_name": x_cu[0].c_name})})

    cu_ord = ord.sum(lambda x_ord: ((({x_ord[0].o_orderkey: record(
        {"c_name": cu_part[x_ord[0].o_custkey].c_name, "o_custkey": x_ord[0].o_custkey,
         "o_orderdate": x_ord[0].o_orderdate, "o_totalprice": x_ord[0].o_totalprice})}) if (
            cu_part[x_ord[0].o_custkey] != None) else (None)) if (True) else (None)) if (
            li_having[x_ord[0].o_orderkey] != None) else (None))

    cu_ord_li = li.sum(lambda x_li: ({record(
        {"c_name": cu_ord[x_li[0].l_orderkey].c_name, "o_custkey": cu_ord[x_li[0].l_orderkey].o_custkey,
         "o_orderkey": x_li[0].l_orderkey, "o_orderdate": cu_ord[x_li[0].l_orderkey].o_orderdate,
         "o_totalprice": cu_ord[x_li[0].l_orderkey].o_totalprice}): record({"sum_quantity": x_li[0].l_quantity})}) if (
            cu_ord[x_li[0].l_orderkey] != None) else (None))

    results = cu_ord_li.sum(lambda x_cu_ord_li: {x_cu_ord_li[0].concat(x_cu_ord_li[1]): True})

    # Insert

    return results
