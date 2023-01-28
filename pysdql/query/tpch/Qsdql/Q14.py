from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    promo = "PROMO"
    pa_part = pa.sum(lambda x_pa: ({x_pa[0].p_partkey: record({"p_type": x_pa[0].p_type})}) if (
        startsWith(x_pa[0].p_type, promo)) else (None))

    pa_li = li.sum(lambda x_li: (record({"A": (((x_li[0].l_extendedprice) * (((1.0) - (x_li[0].l_discount))))) if (
                pa_part[x_li[0].l_partkey] != None) else (0.0),
                                         "B": ((x_li[0].l_extendedprice) * (((1.0) - (x_li[0].l_discount))))})) if (
    ((x_li[0].l_shipdate >= 19950901) * (x_li[0].l_shipdate < 19951001))) else (None))

    results = ((((pa_li.A) / (pa_li.B))) * (100.0))

    # Complete

    return results
