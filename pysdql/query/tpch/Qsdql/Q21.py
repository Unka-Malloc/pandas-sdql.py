from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "na": NATION_TYPE})
def query(su, li, ord, na):
    saudi = "SAUDI ARABIA"
    f = "F"

    nation_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name == saudi, [])

    su_probed = su.joinProbe(
        nation_indexed,
        "s_nationkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.s_suppkey:
                probeDictKey.s_name
        },
        False
    )

    ord_indexed = ord.sum(lambda p:
                          {
                              dense(6000000, unique(p[0].o_orderkey)): True
                          }
                          if
                          p[0].o_orderstatus == f
                          else
                          None
                          )

    l2_indexed = li.sum(
        lambda p:
        {
            dense(6000000, p[0].l_orderkey):
                vector({p[0].l_suppkey})
        }
    )

    l3_indexed = li.sum(
        lambda p:
        {
            dense(6000000, p[0].l_orderkey):
                vector({p[0].l_suppkey})
        }
        if
        p[0].l_receiptdate > p[0].l_commitdate
        else
        None
    )

    l1_probed = li.sum(
        lambda p:
        {
            record(
                {
                    "s_name": su_probed[p[0].l_suppkey]
                }
            )
            :
                record(
                    {
                        "numwait": 1
                    }
                )
        }
        if
        (p[0].l_receiptdate > p[0].l_commitdate) and
        su_probed[p[0].l_suppkey] != None and
        ord_indexed[p[0].l_orderkey] != None and
        (dictSize(l2_indexed[p[0].l_orderkey]) > 1) and
        ((dictSize(l3_indexed[p[0].l_orderkey]) > 0) and (dictSize(l3_indexed[p[0].l_orderkey]) > 1)) == False
        else
        None
    )

    results = l1_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results