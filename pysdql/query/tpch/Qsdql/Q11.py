from pysdql.query.tpch.const import (PARTSUPP_TYPE, SUPPLIER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ps": PARTSUPP_TYPE, "su": SUPPLIER_TYPE, "na": NATION_TYPE})
def query(ps, su, na):

    # Insert

    germany = "GERMANY"

    na_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name == germany, [])

    su_probed = su.joinProbe(
        na_indexed,
        "s_nationkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.s_suppkey: True
        },
        False
    )

    ps_probed = ps.joinProbe(
        su_probed,
        "ps_suppkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        record(
            {
                "A": (probeDictKey.ps_supplycost * probeDictKey.ps_availqty) * 0.0001,
                "B": sr_dict({
                    probeDictKey.ps_partkey: (probeDictKey.ps_supplycost * probeDictKey.ps_availqty)
                })
            })
    )

    results = (ps_probed.B).sum(lambda p:
                                {
                                    record({"ps_partkey": p[0], "value": p[1]}): True
                                }
                                if
                                p[1] > (ps_probed.A)
                                else
                                None
                                )

    # Complete

    return results