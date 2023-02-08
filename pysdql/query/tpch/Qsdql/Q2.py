from pysdql.query.tpch.const import (PART_TYPE, SUPPLIER_TYPE, PARTSUPP_TYPE, NATION_TYPE, REGION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"pa": PART_TYPE, "su": SUPPLIER_TYPE, "ps": PARTSUPP_TYPE, "ps1": PARTSUPP_TYPE, "na": NATION_TYPE, "re": REGION_TYPE})
def query(pa, su, ps, ps1, na, re):

    # INSERT
    brass = "BRASS"
    europe = "EUROPE"

    re_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name == europe, [])

    na_probed = na.joinProbe(
        re_indexed,
        "n_regionkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.n_nationkey: probeDictKey.n_name
        },
        False
    )

    su_probed = su.joinProbe(
        na_probed,
        "s_nationkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.s_suppkey:
                record(
                    {
                        "s_acctbal": probeDictKey.s_acctbal,
                        "s_name": probeDictKey.s_name,
                        "n_name": indexedDictValue,
                        "s_address": probeDictKey.s_address,
                        "s_phone": probeDictKey.s_phone,
                        "s_comment": probeDictKey.s_comment
                    }
                )
        },
        False
    )

    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_size == 15 and endsWith(p[0].p_type, brass), ["p_mfgr"])

    ps_probed = ps.joinProbe(
        su_probed,
        "ps_suppkey",
        lambda p: pa_indexed[p[0].ps_partkey] != None,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.ps_partkey:
                probeDictKey.ps_supplycost  # Min
        }
    )

    results = ps_probed.sum(lambda p: {record({'ps_partkey': p[0], 'min_supplycost': p[1]}): True})

    # results = ps.sum(lambda p:
    #                  {
    #                      unique(record(
    #                          {
    #                              "s_acctbal": su_probed[p[0].ps_suppkey].s_acctbal,
    #                              "s_name": su_probed[p[0].ps_suppkey].s_name,
    #                              "n_name": su_probed[p[0].ps_suppkey].n_name,
    #                              "p_partkey": p[0].ps_partkey,
    #                              "p_mfgr": pa_indexed[p[0].ps_partkey].p_mfgr,
    #                              "s_address": su_probed[p[0].ps_suppkey].s_address,
    #                              "s_phone": su_probed[p[0].ps_suppkey].s_phone,
    #                              "s_comment": su_probed[p[0].ps_suppkey].s_comment
    #                          }
    #                      )):
    #                          True
    #                  }
    #                  if
    #                  ps_probed[p[0].ps_partkey] != None
    #                  and ps_probed[p[0].ps_partkey] == p[0].ps_supplycost
    #                  and su_probed[p[0].ps_suppkey] != None
    #                  else
    #                  None
    #                  )
    # COMPLETE

    return results
