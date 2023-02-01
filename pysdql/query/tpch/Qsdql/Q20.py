from pysdql.query.tpch.const import (SUPPLIER_TYPE, NATION_TYPE, PARTSUPP_TYPE, PART_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "na": NATION_TYPE, "ps": PARTSUPP_TYPE, "pa": PART_TYPE, "li": LINEITEM_TYPE})
def query(su, na, ps, pa, li):

    # Insert

    forest = "forest"
    canada = "CANADA"

    pa_indexed = pa.joinBuild("p_partkey", lambda p: startsWith(p[0].p_name, forest), [])

    na_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name == canada, [])

    su_indexed = su.joinBuild("s_suppkey", lambda p: na_indexed[p[0].s_nationkey] != None, [])

    li_indexed = li.sum(lambda p:
                        {
                            record({
                                "l_partkey": p[0].l_partkey,
                                "l_suppkey": p[0].l_suppkey
                            }):
                                0.5 * p[0].l_quantity
                        }
                        if p[0].l_shipdate >= 19940101 and p[0].l_shipdate < 19950101
                           and pa_indexed[p[0].l_partkey] != None
                           and su_indexed[p[0].l_suppkey] != None
                        else
                        None
                        )

    ps_indexed = ps.joinBuild(
        "ps_suppkey",
        lambda p:
        li_indexed[record({"l_partkey": p[0].ps_partkey, "l_suppkey": p[0].ps_suppkey})] != None and
        p[0].ps_availqty > li_indexed[record({"l_partkey": p[0].ps_partkey, "l_suppkey": p[0].ps_suppkey})],
        []
    )

    results = su.joinProbe(
        ps_indexed,
        "s_suppkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey: {
            record({"s_name": probeDictKey.s_name, "s_address": probeDictKey.s_address}): True},
        False
    )

    # Complete

    return results
