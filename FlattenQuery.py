from pysdql.core.dtypes.sdql_ir import *

from pysdql.extlib.sdqlir_to_sdqlpy import GenerateSDQLPYCode


def q1():
    lineitem_probed = VarExpr("lineitem_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")

    q1 = LetExpr(lineitem_probed, SumBuilder(lambda p: IfExpr((p[0].l_shipdate <= ConstantExpr(19980902)), DicConsExpr([
        (
            RecConsExpr(
                [
                    (
                        "l_returnflag",
                        p[
                            0].l_returnflag),
                    (
                        "l_linestatus",
                        p[
                            0].l_linestatus)]),
            RecConsExpr(
                [
                    (
                        "sum_qty",
                        p[
                            0].l_quantity),
                    (
                        "sum_base_price",
                        p[
                            0].l_extendedprice),
                    (
                        "sum_disc_price",
                        (
                                p[
                                    0].l_extendedprice * (
                                        ConstantExpr(
                                            1.0) -
                                        p[
                                            0].l_discount))),
                    (
                        "sum_charge",
                        (
                                (
                                        p[
                                            0].l_extendedprice * (
                                                ConstantExpr(
                                                    1.0) -
                                                p[
                                                    0].l_discount)) * (
                                        ConstantExpr(
                                            1.0) +
                                        p[
                                            0].l_tax))),
                    (
                        "count_order",
                        ConstantExpr(
                            1))]))]),
                                                              ConstantExpr(None)), li, False), LetExpr(results,
                                                                                                       SumBuilder(lambda
                                                                                                                      p: DicConsExpr(
                                                                                                           [(ConcatExpr(
                                                                                                               p[0],
                                                                                                               p[1]),
                                                                                                             ConstantExpr(
                                                                                                                 True))]),
                                                                                                                  lineitem_probed,
                                                                                                                  True),
                                                                                                       LetExpr(VarExpr(
                                                                                                           "out"),
                                                                                                           results,
                                                                                                           ConstantExpr(
                                                                                                               True))))

    print(q1)


def q2():
    brass = VarExpr("brass")
    europe = VarExpr("europe")
    re_indexed = VarExpr("re_indexed")
    na_probed = VarExpr("na_probed")
    su_probed = VarExpr("su_probed")
    pa_indexed = VarExpr("pa_indexed")
    ps_probed = VarExpr("ps_probed")
    results = VarExpr("results")

    pa = VarExpr("db->pa_dataset")
    su = VarExpr("db->su_dataset")
    ps = VarExpr("db->ps_dataset")
    na = VarExpr("db->na_dataset")
    re = VarExpr("db->re_dataset")

    q2 = LetExpr(brass, ConstantExpr("BRASS"), LetExpr(europe, ConstantExpr("EUROPE"), LetExpr(re_indexed,
                                                                                               JoinPartitionBuilder(re,
                                                                                                                    "r_regionkey",
                                                                                                                    lambda
                                                                                                                        p: (
                                                                                                                            p.r_name == europe),
                                                                                                                    []),
                                                                                               LetExpr(na_probed,
                                                                                                       JoinProbeBuilder(
                                                                                                           re_indexed,
                                                                                                           na,
                                                                                                           "n_regionkey",
                                                                                                           lambda
                                                                                                               p: ConstantExpr(
                                                                                                               True),
                                                                                                           lambda
                                                                                                               indexedDictValue,
                                                                                                               probeDictKey: DicConsExpr(
                                                                                                               [(
                                                                                                                   probeDictKey.n_nationkey,
                                                                                                                   probeDictKey.n_name)]),
                                                                                                           True),
                                                                                                       LetExpr(
                                                                                                           su_probed,
                                                                                                           JoinProbeBuilder(
                                                                                                               na_probed,
                                                                                                               su,
                                                                                                               "s_nationkey",
                                                                                                               lambda
                                                                                                                   p: ConstantExpr(
                                                                                                                   True),
                                                                                                               lambda
                                                                                                                   indexedDictValue,
                                                                                                                   probeDictKey: DicConsExpr(
                                                                                                                   [(
                                                                                                                       probeDictKey.s_suppkey,
                                                                                                                       RecConsExpr(
                                                                                                                           [
                                                                                                                               (
                                                                                                                                   "s_acctbal",
                                                                                                                                   probeDictKey.s_acctbal),
                                                                                                                               (
                                                                                                                                   "s_name",
                                                                                                                                   probeDictKey.s_name),
                                                                                                                               (
                                                                                                                                   "n_name",
                                                                                                                                   indexedDictValue),
                                                                                                                               (
                                                                                                                                   "s_address",
                                                                                                                                   probeDictKey.s_address),
                                                                                                                               (
                                                                                                                                   "s_phone",
                                                                                                                                   probeDictKey.s_phone),
                                                                                                                               (
                                                                                                                                   "s_comment",
                                                                                                                                   probeDictKey.s_comment)]))]),
                                                                                                               True),
                                                                                                           LetExpr(
                                                                                                               pa_indexed,
                                                                                                               JoinPartitionBuilder(
                                                                                                                   pa,
                                                                                                                   "p_partkey",
                                                                                                                   lambda
                                                                                                                       p: (
                                                                                                                           (
                                                                                                                               (
                                                                                                                                       p.p_size == ConstantExpr(
                                                                                                                                   15))) * (
                                                                                                                               ExtFuncExpr(
                                                                                                                                   ExtFuncSymbol.EndsWith,
                                                                                                                                   p.p_type,
                                                                                                                                   brass,
                                                                                                                                   ConstantExpr(
                                                                                                                                       "Nothing!")))),
                                                                                                                   [
                                                                                                                       "p_mfgr"]),
                                                                                                               LetExpr(
                                                                                                                   ps_probed,
                                                                                                                   JoinProbeBuilder(
                                                                                                                       su_probed,
                                                                                                                       ps,
                                                                                                                       "ps_suppkey",
                                                                                                                       lambda
                                                                                                                           p: (
                                                                                                                               pa_indexed[
                                                                                                                                   p.ps_partkey] != ConstantExpr(
                                                                                                                           None)),
                                                                                                                       lambda
                                                                                                                           indexedDictValue,
                                                                                                                           probeDictKey: DicConsExpr(
                                                                                                                           [
                                                                                                                               (
                                                                                                                                   probeDictKey.ps_partkey,
                                                                                                                                   probeDictKey.ps_supplycost)])),
                                                                                                                   LetExpr(
                                                                                                                       results,
                                                                                                                       SumBuilder(
                                                                                                                           lambda
                                                                                                                               p: IfExpr(
                                                                                                                               (
                                                                                                                                       (
                                                                                                                                           (
                                                                                                                                                   ps_probed[
                                                                                                                                                       p[
                                                                                                                                                           0].ps_partkey] != ConstantExpr(
                                                                                                                                               None))) * (
                                                                                                                                           (
                                                                                                                                                   ps_probed[
                                                                                                                                                       p[
                                                                                                                                                           0].ps_partkey] ==
                                                                                                                                                   p[
                                                                                                                                                       0].ps_supplycost)) * (
                                                                                                                                           (
                                                                                                                                                   su_probed[
                                                                                                                                                       p[
                                                                                                                                                           0].ps_suppkey] != ConstantExpr(
                                                                                                                                               None)))),
                                                                                                                               DicConsExpr(
                                                                                                                                   [
                                                                                                                                       (
                                                                                                                                           RecConsExpr(
                                                                                                                                               [
                                                                                                                                                   (
                                                                                                                                                       "s_acctbal",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_suppkey].s_acctbal),
                                                                                                                                                   (
                                                                                                                                                       "s_name",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_suppkey].s_name),
                                                                                                                                                   (
                                                                                                                                                       "n_name",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_suppkey].n_name),
                                                                                                                                                   (
                                                                                                                                                       "p_partkey",
                                                                                                                                                       p[
                                                                                                                                                           0].ps_partkey),
                                                                                                                                                   (
                                                                                                                                                       "p_mfgr",
                                                                                                                                                       pa_indexed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_partkey].p_mfgr),
                                                                                                                                                   (
                                                                                                                                                       "s_address",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_suppkey].s_address),
                                                                                                                                                   (
                                                                                                                                                       "s_phone",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_suppkey].s_phone),
                                                                                                                                                   (
                                                                                                                                                       "s_comment",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].ps_suppkey].s_comment)]),
                                                                                                                                           ConstantExpr(
                                                                                                                                               True))]),
                                                                                                                               ConstantExpr(
                                                                                                                                   None)),
                                                                                                                           ps,
                                                                                                                           True),
                                                                                                                       LetExpr(
                                                                                                                           VarExpr(
                                                                                                                               "out"),
                                                                                                                           results,
                                                                                                                           ConstantExpr(
                                                                                                                               True))))))))))

    print(q2)


def q3():
    building = VarExpr("building")
    customer_indexed = VarExpr("customer_indexed")
    order_probed = VarExpr("order_probed")
    lineitem_probed = VarExpr("lineitem_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    cu = VarExpr("db->cu_dataset")
    ord = VarExpr("db->ord_dataset")

    q3 = LetExpr(building, ConstantExpr("BUILDING"),
                 LetExpr(customer_indexed,
                         JoinPartitionBuilder(cu, "c_custkey", lambda p: (p.c_mktsegment == building), []),
                         LetExpr(order_probed, JoinProbeBuilder(customer_indexed, ord, "o_custkey",
                                                                lambda p: (p.o_orderdate < ConstantExpr(19950315)),
                                                                lambda indexedDictValue, probeDictKey: DicConsExpr([(
                                                                    probeDictKey.o_orderkey,
                                                                    RecConsExpr(
                                                                        [(
                                                                            "o_orderdate",
                                                                            probeDictKey.o_orderdate),
                                                                            (
                                                                                "o_shippriority",
                                                                                probeDictKey.o_shippriority)]))]),
                                                                True), LetExpr(lineitem_probed,
                                                                               JoinProbeBuilder(order_probed, li,
                                                                                                "l_orderkey",
                                                                                                lambda p: (
                                                                                                        p.l_shipdate > ConstantExpr(
                                                                                                    19950315)),
                                                                                                lambda indexedDictValue,
                                                                                                       probeDictKey: DicConsExpr(
                                                                                                    [(RecConsExpr([(
                                                                                                        "l_orderkey",
                                                                                                        probeDictKey.l_orderkey),
                                                                                                        (
                                                                                                            "o_orderdate",
                                                                                                            indexedDictValue.o_orderdate),
                                                                                                        (
                                                                                                            "o_shippriority",
                                                                                                            indexedDictValue.o_shippriority)]),
                                                                                                      RecConsExpr([(
                                                                                                          "revenue",
                                                                                                          (
                                                                                                                  probeDictKey.l_extendedprice * (
                                                                                                                  ConstantExpr(
                                                                                                                      1.0) - probeDictKey.l_discount)))]))])),
                                                                               LetExpr(results, SumBuilder(
                                                                                   lambda p: DicConsExpr(
                                                                                       [(ConcatExpr(p[0],
                                                                                                    p[1]),
                                                                                         ConstantExpr(
                                                                                             True))]),
                                                                                   lineitem_probed, True),
                                                                                       LetExpr(VarExpr("out"), results,
                                                                                               ConstantExpr(True)))))))

    print(q3)

    '''
    LetExpr(building, ConstantExpr("BUILDING"), LetExpr(customer_indexed, SumExpr(v1, db->cu_dataset, IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'c_mktsegment'), building), DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'c_custkey'), RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(v1, 0), 'c_custkey'))]))]), EmptyDicConsExpr()), True), LetExpr(order_probed, LetExpr(v3, customer_indexed, SumExpr(v4, db->ord_dataset, IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderdate'), ConstantExpr(19950315)), IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v3, RecAccessExpr(PairAccessExpr(v4, 0), 'o_custkey')), ConstantExpr(None)), DicConsExpr([(RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderkey'), RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderdate')), ('o_shippriority', RecAccessExpr(PairAccessExpr(v4, 0), 'o_shippriority'))]))]), EmptyDicConsExpr()), EmptyDicConsExpr()), True)), LetExpr(lineitem_probed, LetExpr(v6, order_probed, SumExpr(v7, db->li_dataset, IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(v7, 0), 'l_shipdate'), ConstantExpr(19950315)), IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), ConstantExpr(None)), DicConsExpr([(RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), ('o_orderdate', RecAccessExpr(DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 'o_orderdate')), ('o_shippriority', RecAccessExpr(DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 'o_shippriority'))]), RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(v7, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v7, 0), 'l_discount'))))]))]), EmptyDicConsExpr()), EmptyDicConsExpr()), False)), LetExpr(results, SumExpr(v9, lineitem_probed, DicConsExpr([(ConcatExpr(PairAccessExpr(v9, 0), PairAccessExpr(v9, 1)), ConstantExpr(True))]), True), LetExpr(out, results, ConstantExpr(True)))))))
    '''


def q4():
    li_indexed = VarExpr("li_indexed")
    ord_probed = VarExpr("ord_probed")
    results = VarExpr("results")

    ord = VarExpr("db->ord_dataset")
    li = VarExpr("db->li_dataset")

    q4 = LetExpr(li_indexed, SumBuilder(
        lambda p: IfExpr((p[0].l_commitdate < p[0].l_receiptdate), DicConsExpr([(p[0].l_orderkey, ConstantExpr(True))]),
                         ConstantExpr(None)), li, True, "dense_array(6000000)"), LetExpr(ord_probed,
                                                                                         JoinProbeBuilder(li_indexed,
                                                                                                          ord,
                                                                                                          "o_orderkey",
                                                                                                          lambda p: (((
                                                                                                                  p.o_orderdate >= ConstantExpr(
                                                                                                              19930701))) * (
                                                                                                                         (
                                                                                                                                 p.o_orderdate < ConstantExpr(
                                                                                                                             19931001)))),
                                                                                                          lambda
                                                                                                              indexedDictValue,
                                                                                                              probeDictKey: DicConsExpr(
                                                                                                              [(
                                                                                                                  probeDictKey.o_orderpriority,
                                                                                                                  ConstantExpr(
                                                                                                                      1))])),
                                                                                         LetExpr(results, SumBuilder(
                                                                                             lambda p: DicConsExpr([(
                                                                                                 RecConsExpr(
                                                                                                     [
                                                                                                         (
                                                                                                             "o_orderpriority",
                                                                                                             p[
                                                                                                                 0]),
                                                                                                         (
                                                                                                             "order_count",
                                                                                                             p[
                                                                                                                 1])]),
                                                                                                 ConstantExpr(
                                                                                                     True))]),
                                                                                             ord_probed, True),
                                                                                                 LetExpr(VarExpr("out"),
                                                                                                         results,
                                                                                                         ConstantExpr(
                                                                                                             True)))))

    print(q4)


def q5():
    asia = VarExpr("asia")
    region_indexed = VarExpr("region_indexed")
    nation_probed = VarExpr("nation_probed")
    customer_probed = VarExpr("customer_probed")
    order_probed = VarExpr("order_probed")
    supplier_project = VarExpr("supplier_project")
    lineitem_probed = VarExpr("lineitem_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    cu = VarExpr("db->cu_dataset")
    ord = VarExpr("db->ord_dataset")
    re = VarExpr("db->re_dataset")
    na = VarExpr("db->na_dataset")
    su = VarExpr("db->su_dataset")

    q5 = LetExpr(asia, ConstantExpr("ASIA"),
                 LetExpr(region_indexed, JoinPartitionBuilder(re, "r_regionkey", lambda p: (p.r_name == asia), []),
                         LetExpr(nation_probed,
                                 JoinProbeBuilder(region_indexed, na, "n_regionkey", lambda p: ConstantExpr(True),
                                                  lambda indexedDictValue, probeDictKey: DicConsExpr(
                                                      [(probeDictKey.n_nationkey, probeDictKey.n_name)]), True),
                                 LetExpr(customer_probed, JoinProbeBuilder(nation_probed, cu, "c_nationkey",
                                                                           lambda p: ConstantExpr(True),
                                                                           lambda indexedDictValue,
                                                                                  probeDictKey: DicConsExpr([(
                                                                               probeDictKey.c_custkey,
                                                                               RecConsExpr(
                                                                                   [(
                                                                                       "n_name",
                                                                                       indexedDictValue),
                                                                                       (
                                                                                           "c_nationkey",
                                                                                           probeDictKey.c_nationkey)]))]),
                                                                           True), LetExpr(order_probed,
                                                                                          JoinProbeBuilder(
                                                                                              customer_probed, ord,
                                                                                              "o_custkey", lambda p: ((
                                                                                                                              p.o_orderdate < ConstantExpr(
                                                                                                                          19950101)) * (
                                                                                                                              p.o_orderdate >= ConstantExpr(
                                                                                                                          19940101))),
                                                                                              lambda indexedDictValue,
                                                                                                     probeDictKey: DicConsExpr(
                                                                                                  [(
                                                                                                      probeDictKey.o_orderkey,
                                                                                                      RecConsExpr([(
                                                                                                          "n_name",
                                                                                                          indexedDictValue.n_name),
                                                                                                          (
                                                                                                              "c_nationkey",
                                                                                                              indexedDictValue.c_nationkey)]))]),
                                                                                              True),
                                                                                          LetExpr(supplier_project,
                                                                                                  SumBuilder(lambda
                                                                                                                 p: DicConsExpr(
                                                                                                      [(RecConsExpr([(
                                                                                                          "s_suppkey",
                                                                                                          p[
                                                                                                              0].s_suppkey),
                                                                                                          (
                                                                                                              "s_nationkey",
                                                                                                              p[
                                                                                                                  0].s_nationkey)]),
                                                                                                        ConstantExpr(
                                                                                                            True))]),
                                                                                                             su, True),
                                                                                                  LetExpr(
                                                                                                      lineitem_probed,
                                                                                                      JoinProbeBuilder(
                                                                                                          order_probed,
                                                                                                          li,
                                                                                                          "l_orderkey",
                                                                                                          lambda
                                                                                                              p: ConstantExpr(
                                                                                                              True),
                                                                                                          lambda
                                                                                                              indexedDictValue,
                                                                                                              probeDictKey: IfExpr(
                                                                                                              (
                                                                                                                      supplier_project[
                                                                                                                          RecConsExpr(
                                                                                                                              [
                                                                                                                                  (
                                                                                                                                      "l_suppkey",
                                                                                                                                      probeDictKey.l_suppkey),
                                                                                                                                  (
                                                                                                                                      "c_nationkey",
                                                                                                                                      indexedDictValue.c_nationkey)])] != ConstantExpr(
                                                                                                                  None)),
                                                                                                              DicConsExpr(
                                                                                                                  [(
                                                                                                                      indexedDictValue.n_name,
                                                                                                                      (
                                                                                                                              probeDictKey.l_extendedprice * (
                                                                                                                              ConstantExpr(
                                                                                                                                  1.0) - probeDictKey.l_discount)))]),
                                                                                                              ConstantExpr(
                                                                                                                  None))),
                                                                                                      LetExpr(results,
                                                                                                              SumBuilder(
                                                                                                                  lambda
                                                                                                                      p: DicConsExpr(
                                                                                                                      [(
                                                                                                                          RecConsExpr(
                                                                                                                              [
                                                                                                                                  (
                                                                                                                                      "n_name",
                                                                                                                                      p[
                                                                                                                                          0]),
                                                                                                                                  (
                                                                                                                                      "revenue",
                                                                                                                                      p[
                                                                                                                                          1])]),
                                                                                                                          ConstantExpr(
                                                                                                                              True))]),
                                                                                                                  lineitem_probed,
                                                                                                                  True),
                                                                                                              LetExpr(
                                                                                                                  VarExpr(
                                                                                                                      "out"),
                                                                                                                  results,
                                                                                                                  ConstantExpr(
                                                                                                                      True))))))))))

    print(q5)


def q6():
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")

    q6 = LetExpr(results, SumBuilder(lambda p: IfExpr((((p[0].l_shipdate >= ConstantExpr(19940101))) * (
        (p[0].l_shipdate < ConstantExpr(19950101))) * ((p[0].l_discount >= ConstantExpr(0.05))) * (
                                                           (p[0].l_discount <= ConstantExpr(0.07))) * (
                                                           (p[0].l_quantity < ConstantExpr(24.0)))),
                                                      (p[0].l_extendedprice * p[0].l_discount), ConstantExpr(0.0)),
                                     li, ), LetExpr(VarExpr("out"), results, ConstantExpr(True)))

    print(q6)


def q7():
    france = VarExpr("france")
    germany = VarExpr("germany")
    nation_indexed = VarExpr("nation_indexed")
    cu_probed = VarExpr("cu_probed")
    ord_probed = VarExpr("ord_probed")
    su_probed = VarExpr("su_probed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    su = VarExpr("db->su_dataset")
    li = VarExpr("db->li_dataset")
    ord = VarExpr("db->ord_dataset")
    cu = VarExpr("db->cu_dataset")
    na = VarExpr("db->na_dataset")

    q7 = LetExpr(france, ConstantExpr("FRANCE"), LetExpr(germany, ConstantExpr("GERMANY"), LetExpr(nation_indexed,
                                                                                                   JoinPartitionBuilder(
                                                                                                       na,
                                                                                                       "n_nationkey",
                                                                                                       lambda p: (((
                                                                                                               p.n_name == france)) + (
                                                                                                                      (
                                                                                                                              p.n_name == germany))),
                                                                                                       ["n_name"]),
                                                                                                   LetExpr(cu_probed,
                                                                                                           JoinProbeBuilder(
                                                                                                               nation_indexed,
                                                                                                               cu,
                                                                                                               "c_nationkey",
                                                                                                               lambda
                                                                                                                   p: ConstantExpr(
                                                                                                                   True),
                                                                                                               lambda
                                                                                                                   indexedDictValue,
                                                                                                                   probeDictKey: DicConsExpr(
                                                                                                                   [(
                                                                                                                       probeDictKey.c_custkey,
                                                                                                                       indexedDictValue.n_name)]),
                                                                                                               True),
                                                                                                           LetExpr(
                                                                                                               ord_probed,
                                                                                                               JoinProbeBuilder(
                                                                                                                   cu_probed,
                                                                                                                   ord,
                                                                                                                   "o_custkey",
                                                                                                                   lambda
                                                                                                                       p: ConstantExpr(
                                                                                                                       True),
                                                                                                                   lambda
                                                                                                                       indexedDictValue,
                                                                                                                       probeDictKey: DicConsExpr(
                                                                                                                       [
                                                                                                                           (
                                                                                                                               probeDictKey.o_orderkey,
                                                                                                                               indexedDictValue)]),
                                                                                                                   True),
                                                                                                               LetExpr(
                                                                                                                   su_probed,
                                                                                                                   JoinProbeBuilder(
                                                                                                                       nation_indexed,
                                                                                                                       su,
                                                                                                                       "s_nationkey",
                                                                                                                       lambda
                                                                                                                           p: ConstantExpr(
                                                                                                                           True),
                                                                                                                       lambda
                                                                                                                           indexedDictValue,
                                                                                                                           probeDictKey: DicConsExpr(
                                                                                                                           [
                                                                                                                               (
                                                                                                                                   probeDictKey.s_suppkey,
                                                                                                                                   indexedDictValue.n_name)]),
                                                                                                                       True),
                                                                                                                   LetExpr(
                                                                                                                       li_probed,
                                                                                                                       SumBuilder(
                                                                                                                           lambda
                                                                                                                               p: IfExpr(
                                                                                                                               (
                                                                                                                                       (
                                                                                                                                           (
                                                                                                                                                   p[
                                                                                                                                                       0].l_shipdate >= ConstantExpr(
                                                                                                                                               19950101))) * (
                                                                                                                                           (
                                                                                                                                                   p[
                                                                                                                                                       0].l_shipdate <= ConstantExpr(
                                                                                                                                               19961231))) * (
                                                                                                                                           (
                                                                                                                                                   ord_probed[
                                                                                                                                                       p[
                                                                                                                                                           0].l_orderkey] != ConstantExpr(
                                                                                                                                               None))) * (
                                                                                                                                           (
                                                                                                                                                   su_probed[
                                                                                                                                                       p[
                                                                                                                                                           0].l_suppkey] != ConstantExpr(
                                                                                                                                               None))) * (
                                                                                                                                           (
                                                                                                                                                   (
                                                                                                                                                       (
                                                                                                                                                               (
                                                                                                                                                                   (
                                                                                                                                                                           ord_probed[
                                                                                                                                                                               p[
                                                                                                                                                                                   0].l_orderkey] == france)) * (
                                                                                                                                                                   (
                                                                                                                                                                           su_probed[
                                                                                                                                                                               p[
                                                                                                                                                                                   0].l_suppkey] == germany)))) + (
                                                                                                                                                       (
                                                                                                                                                               (
                                                                                                                                                                   (
                                                                                                                                                                           ord_probed[
                                                                                                                                                                               p[
                                                                                                                                                                                   0].l_orderkey] == germany)) * (
                                                                                                                                                                   (
                                                                                                                                                                           su_probed[
                                                                                                                                                                               p[
                                                                                                                                                                                   0].l_suppkey] == france))))))),
                                                                                                                               DicConsExpr(
                                                                                                                                   [
                                                                                                                                       (
                                                                                                                                           RecConsExpr(
                                                                                                                                               [
                                                                                                                                                   (
                                                                                                                                                       "supp_nation",
                                                                                                                                                       su_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].l_suppkey]),
                                                                                                                                                   (
                                                                                                                                                       "cust_nation",
                                                                                                                                                       ord_probed[
                                                                                                                                                           p[
                                                                                                                                                               0].l_orderkey]),
                                                                                                                                                   (
                                                                                                                                                       "l_year",
                                                                                                                                                       ExtFuncExpr(
                                                                                                                                                           ExtFuncSymbol.ExtractYear,
                                                                                                                                                           p[
                                                                                                                                                               0].l_shipdate,
                                                                                                                                                           ConstantExpr(
                                                                                                                                                               "Nothing!"),
                                                                                                                                                           ConstantExpr(
                                                                                                                                                               "Nothing!")))]),
                                                                                                                                           RecConsExpr(
                                                                                                                                               [
                                                                                                                                                   (
                                                                                                                                                       "revenue",
                                                                                                                                                       (
                                                                                                                                                               p[
                                                                                                                                                                   0].l_extendedprice * (
                                                                                                                                                                       ConstantExpr(
                                                                                                                                                                           1.0) -
                                                                                                                                                                       p[
                                                                                                                                                                           0].l_discount)))]))]),
                                                                                                                               ConstantExpr(
                                                                                                                                   None)),
                                                                                                                           li,
                                                                                                                           False),
                                                                                                                       LetExpr(
                                                                                                                           results,
                                                                                                                           SumBuilder(
                                                                                                                               lambda
                                                                                                                                   p: DicConsExpr(
                                                                                                                                   [
                                                                                                                                       (
                                                                                                                                           ConcatExpr(
                                                                                                                                               p[
                                                                                                                                                   0],
                                                                                                                                               p[
                                                                                                                                                   1]),
                                                                                                                                           ConstantExpr(
                                                                                                                                               True))]),
                                                                                                                               li_probed,
                                                                                                                               True),
                                                                                                                           LetExpr(
                                                                                                                               VarExpr(
                                                                                                                                   "out"),
                                                                                                                               results,
                                                                                                                               ConstantExpr(
                                                                                                                                   True))))))))))

    print(q7)


def q8():
    steel = VarExpr("steel")
    america = VarExpr("america")
    brazil = VarExpr("brazil")
    re_indexed = VarExpr("re_indexed")
    na_probed = VarExpr("na_probed")
    na_indexed = VarExpr("na_indexed")
    su_indexed = VarExpr("su_indexed")
    cu_indexed = VarExpr("cu_indexed")
    pa_indexed = VarExpr("pa_indexed")
    ord_indexed = VarExpr("ord_indexed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    pa = VarExpr("db->pa_dataset")
    su = VarExpr("db->su_dataset")
    li = VarExpr("db->li_dataset")
    ord = VarExpr("db->ord_dataset")
    cu = VarExpr("db->cu_dataset")
    na = VarExpr("db->na_dataset")
    re = VarExpr("db->re_dataset")

    q8 = LetExpr(steel, ConstantExpr("ECONOMY ANODIZED STEEL"), LetExpr(america, ConstantExpr("AMERICA"),
                                                                        LetExpr(brazil, ConstantExpr("BRAZIL"),
                                                                                LetExpr(re_indexed,
                                                                                        JoinPartitionBuilder(re,
                                                                                                             "r_regionkey",
                                                                                                             lambda p: (
                                                                                                                     p.r_name == america),
                                                                                                             []),
                                                                                        LetExpr(na_probed,
                                                                                                JoinProbeBuilder(
                                                                                                    re_indexed, na,
                                                                                                    "n_regionkey",
                                                                                                    lambda
                                                                                                        p: ConstantExpr(
                                                                                                        True), lambda
                                                                                                        indexedDictValue,
                                                                                                        probeDictKey: DicConsExpr(
                                                                                                        [(
                                                                                                            probeDictKey.n_nationkey,
                                                                                                            ConstantExpr(
                                                                                                                True))]),
                                                                                                    True),
                                                                                                LetExpr(na_indexed,
                                                                                                        JoinPartitionBuilder(
                                                                                                            na,
                                                                                                            "n_nationkey",
                                                                                                            lambda
                                                                                                                p: ConstantExpr(
                                                                                                                True),
                                                                                                            ["n_name"]),
                                                                                                        LetExpr(
                                                                                                            su_indexed,
                                                                                                            JoinPartitionBuilder(
                                                                                                                su,
                                                                                                                "s_suppkey",
                                                                                                                lambda
                                                                                                                    p: ConstantExpr(
                                                                                                                    True),
                                                                                                                [
                                                                                                                    "s_nationkey"]),
                                                                                                            LetExpr(
                                                                                                                cu_indexed,
                                                                                                                SumBuilder(
                                                                                                                    lambda
                                                                                                                        p: DicConsExpr(
                                                                                                                        [
                                                                                                                            (
                                                                                                                                p[
                                                                                                                                    0].c_custkey,
                                                                                                                                p[
                                                                                                                                    0].c_nationkey)]),
                                                                                                                    cu,
                                                                                                                    True,
                                                                                                                    "dense_array(200000)"),
                                                                                                                LetExpr(
                                                                                                                    pa_indexed,
                                                                                                                    JoinPartitionBuilder(
                                                                                                                        pa,
                                                                                                                        "p_partkey",
                                                                                                                        lambda
                                                                                                                            p: (
                                                                                                                                p.p_type == steel),
                                                                                                                        []),
                                                                                                                    LetExpr(
                                                                                                                        ord_indexed,
                                                                                                                        JoinPartitionBuilder(
                                                                                                                            ord,
                                                                                                                            "o_orderkey",
                                                                                                                            lambda
                                                                                                                                p: (
                                                                                                                                    (
                                                                                                                                        (
                                                                                                                                                p.o_orderdate >= ConstantExpr(
                                                                                                                                            19950101))) * (
                                                                                                                                        (
                                                                                                                                                p.o_orderdate <= ConstantExpr(
                                                                                                                                            19961231)))),
                                                                                                                            [
                                                                                                                                "o_custkey",
                                                                                                                                "o_orderdate"]),
                                                                                                                        LetExpr(
                                                                                                                            li_probed,
                                                                                                                            JoinProbeBuilder(
                                                                                                                                pa_indexed,
                                                                                                                                li,
                                                                                                                                "l_partkey",
                                                                                                                                lambda
                                                                                                                                    p: ConstantExpr(
                                                                                                                                    True),
                                                                                                                                lambda
                                                                                                                                    indexedDictValue,
                                                                                                                                    probeDictKey: IfExpr(
                                                                                                                                    (
                                                                                                                                            (
                                                                                                                                                (
                                                                                                                                                        ord_indexed[
                                                                                                                                                            probeDictKey.l_orderkey] != ConstantExpr(
                                                                                                                                                    None))) * (
                                                                                                                                                (
                                                                                                                                                        na_probed[
                                                                                                                                                            cu_indexed[
                                                                                                                                                                ord_indexed[
                                                                                                                                                                    probeDictKey.l_orderkey].o_custkey]] != ConstantExpr(
                                                                                                                                                    None)))),
                                                                                                                                    DicConsExpr(
                                                                                                                                        [
                                                                                                                                            (
                                                                                                                                                ExtFuncExpr(
                                                                                                                                                    ExtFuncSymbol.ExtractYear,
                                                                                                                                                    ord_indexed[
                                                                                                                                                        probeDictKey.l_orderkey].o_orderdate,
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        "Nothing!"),
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        "Nothing!")),
                                                                                                                                                RecConsExpr(
                                                                                                                                                    [
                                                                                                                                                        (
                                                                                                                                                            "A",
                                                                                                                                                            IfExpr(
                                                                                                                                                                (
                                                                                                                                                                        na_indexed[
                                                                                                                                                                            su_indexed[
                                                                                                                                                                                probeDictKey.l_suppkey].s_nationkey].n_name == brazil),
                                                                                                                                                                (
                                                                                                                                                                        probeDictKey.l_extendedprice * (
                                                                                                                                                                        ConstantExpr(
                                                                                                                                                                            1.0) - probeDictKey.l_discount)),
                                                                                                                                                                ConstantExpr(
                                                                                                                                                                    0.0))),
                                                                                                                                                        (
                                                                                                                                                            "B",
                                                                                                                                                            (
                                                                                                                                                                    probeDictKey.l_extendedprice * (
                                                                                                                                                                    ConstantExpr(
                                                                                                                                                                        1.0) - probeDictKey.l_discount)))]))]),
                                                                                                                                    ConstantExpr(
                                                                                                                                        None))),
                                                                                                                            LetExpr(
                                                                                                                                results,
                                                                                                                                SumBuilder(
                                                                                                                                    lambda
                                                                                                                                        p: DicConsExpr(
                                                                                                                                        [
                                                                                                                                            (
                                                                                                                                                RecConsExpr(
                                                                                                                                                    [
                                                                                                                                                        (
                                                                                                                                                            "o_year",
                                                                                                                                                            p[
                                                                                                                                                                0]),
                                                                                                                                                        (
                                                                                                                                                            "mkt_share",
                                                                                                                                                            (
                                                                                                                                                                    p[
                                                                                                                                                                        1].A /
                                                                                                                                                                    p[
                                                                                                                                                                        1].B))]),
                                                                                                                                                ConstantExpr(
                                                                                                                                                    True))]),
                                                                                                                                    li_probed,
                                                                                                                                    True),
                                                                                                                                LetExpr(
                                                                                                                                    VarExpr(
                                                                                                                                        "out"),
                                                                                                                                    results,
                                                                                                                                    ConstantExpr(
                                                                                                                                        True))))))))))))))
    print(q8)


def q9():
    nation_indexed = VarExpr("nation_indexed")
    supplier_probed = VarExpr("supplier_probed")
    green = VarExpr("green")
    part_indexed = VarExpr("part_indexed")
    partsupp_probe = VarExpr("partsupp_probe")
    ord_indexed = VarExpr("ord_indexed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    ord = VarExpr("db->ord_dataset")
    na = VarExpr("db->na_dataset")
    su = VarExpr("db->su_dataset")
    pa = VarExpr("db->pa_dataset")
    ps = VarExpr("db->ps_dataset")

    q9 = LetExpr(nation_indexed, JoinPartitionBuilder(na, "n_nationkey", lambda p: ConstantExpr(True), ["n_name"]),
                 LetExpr(supplier_probed,
                         SumBuilder(lambda p: DicConsExpr([(p[0].s_suppkey, nation_indexed[p[0].s_nationkey].n_name)]),
                                    su, True), LetExpr(green, ConstantExpr("green"), LetExpr(part_indexed,
                                                                                             JoinPartitionBuilder(pa,
                                                                                                                  "p_partkey",
                                                                                                                  lambda
                                                                                                                      p: ExtFuncExpr(
                                                                                                                      ExtFuncSymbol.StringContains,
                                                                                                                      green,
                                                                                                                      ConstantExpr(
                                                                                                                          -1),
                                                                                                                      p.p_name) == ConstantExpr(
                                                                                                                      True),
                                                                                                                  []),
                                                                                             LetExpr(partsupp_probe,
                                                                                                     JoinProbeBuilder(
                                                                                                         part_indexed,
                                                                                                         ps,
                                                                                                         "ps_partkey",
                                                                                                         lambda
                                                                                                             p: ConstantExpr(
                                                                                                             True),
                                                                                                         lambda
                                                                                                             indexedDictValue,
                                                                                                             probeDictKey: DicConsExpr(
                                                                                                             [(
                                                                                                                 RecConsExpr(
                                                                                                                     [(
                                                                                                                         "ps_partkey",
                                                                                                                         probeDictKey.ps_partkey),
                                                                                                                         (
                                                                                                                             "ps_suppkey",
                                                                                                                             probeDictKey.ps_suppkey)]),
                                                                                                                 RecConsExpr(
                                                                                                                     [(
                                                                                                                         "n_name",
                                                                                                                         supplier_probed[
                                                                                                                             probeDictKey.ps_suppkey]),
                                                                                                                         (
                                                                                                                             "ps_supplycost",
                                                                                                                             probeDictKey.ps_supplycost)]))]),
                                                                                                         True), LetExpr(
                                                                                                     ord_indexed,
                                                                                                     SumBuilder(lambda
                                                                                                                    p: DicConsExpr(
                                                                                                         [(p[
                                                                                                               0].o_orderkey,
                                                                                                           p[
                                                                                                               0].o_orderdate)]),
                                                                                                                ord,
                                                                                                                True,
                                                                                                                "dense_array(6000000)"),
                                                                                                     LetExpr(li_probed,
                                                                                                             SumBuilder(
                                                                                                                 lambda
                                                                                                                     p: IfExpr(
                                                                                                                     (
                                                                                                                             partsupp_probe[
                                                                                                                                 RecConsExpr(
                                                                                                                                     [
                                                                                                                                         (
                                                                                                                                             "ps_partkey",
                                                                                                                                             p[
                                                                                                                                                 0].l_partkey),
                                                                                                                                         (
                                                                                                                                             "ps_suppkey",
                                                                                                                                             p[
                                                                                                                                                 0].l_suppkey)])] != ConstantExpr(
                                                                                                                         None)),
                                                                                                                     DicConsExpr(
                                                                                                                         [
                                                                                                                             (
                                                                                                                                 RecConsExpr(
                                                                                                                                     [
                                                                                                                                         (
                                                                                                                                             "nation",
                                                                                                                                             partsupp_probe[
                                                                                                                                                 RecConsExpr(
                                                                                                                                                     [
                                                                                                                                                         (
                                                                                                                                                             "ps_partkey",
                                                                                                                                                             p[
                                                                                                                                                                 0].l_partkey),
                                                                                                                                                         (
                                                                                                                                                             "ps_suppkey",
                                                                                                                                                             p[
                                                                                                                                                                 0].l_suppkey)])].n_name),
                                                                                                                                         (
                                                                                                                                             "o_year",
                                                                                                                                             ExtFuncExpr(
                                                                                                                                                 ExtFuncSymbol.ExtractYear,
                                                                                                                                                 ord_indexed[
                                                                                                                                                     p[
                                                                                                                                                         0].l_orderkey],
                                                                                                                                                 ConstantExpr(
                                                                                                                                                     "Nothing!"),
                                                                                                                                                 ConstantExpr(
                                                                                                                                                     "Nothing!")))]),
                                                                                                                                 RecConsExpr(
                                                                                                                                     [
                                                                                                                                         (
                                                                                                                                             "sum_profit",
                                                                                                                                             (
                                                                                                                                                     (
                                                                                                                                                             p[
                                                                                                                                                                 0].l_extendedprice * (
                                                                                                                                                                     ConstantExpr(
                                                                                                                                                                         1.0) -
                                                                                                                                                                     p[
                                                                                                                                                                         0].l_discount)) - (
                                                                                                                                                             partsupp_probe[
                                                                                                                                                                 RecConsExpr(
                                                                                                                                                                     [
                                                                                                                                                                         (
                                                                                                                                                                             "ps_partkey",
                                                                                                                                                                             p[
                                                                                                                                                                                 0].l_partkey),
                                                                                                                                                                         (
                                                                                                                                                                             "ps_suppkey",
                                                                                                                                                                             p[
                                                                                                                                                                                 0].l_suppkey)])].ps_supplycost *
                                                                                                                                                             p[
                                                                                                                                                                 0].l_quantity)))]))]),
                                                                                                                     ConstantExpr(
                                                                                                                         None)),
                                                                                                                 li, ),
                                                                                                             LetExpr(
                                                                                                                 results,
                                                                                                                 SumBuilder(
                                                                                                                     lambda
                                                                                                                         p: DicConsExpr(
                                                                                                                         [
                                                                                                                             (
                                                                                                                                 ConcatExpr(
                                                                                                                                     p[
                                                                                                                                         0],
                                                                                                                                     p[
                                                                                                                                         1]),
                                                                                                                                 ConstantExpr(
                                                                                                                                     True))]),
                                                                                                                     li_probed,
                                                                                                                     True),
                                                                                                                 LetExpr(
                                                                                                                     VarExpr(
                                                                                                                         "out"),
                                                                                                                     results,
                                                                                                                     ConstantExpr(
                                                                                                                         True))))))))))

    print(q9)


def q10():
    r = VarExpr("r")
    na_indexed = VarExpr("na_indexed")
    cu_indexed = VarExpr("cu_indexed")
    ord_probed = VarExpr("ord_probed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    cu = VarExpr("db->cu_dataset")
    ord = VarExpr("db->ord_dataset")
    li = VarExpr("db->li_dataset")
    na = VarExpr("db->na_dataset")

    q10 = LetExpr(r, ConstantExpr("R"), LetExpr(na_indexed,
                                                JoinPartitionBuilder(na, "n_nationkey", lambda p: ConstantExpr(True),
                                                                     ["n_name"], "phmap::flat_hash_map"),
                                                LetExpr(cu_indexed, JoinPartitionBuilder(cu, "c_custkey",
                                                                                         lambda p: ConstantExpr(True),
                                                                                         ["c_custkey", "c_name",
                                                                                          "c_acctbal", "c_address",
                                                                                          "c_nationkey", "c_phone",
                                                                                          "c_comment"],
                                                                                         "phmap::flat_hash_map"),
                                                        LetExpr(ord_probed,
                                                                JoinProbeBuilder(cu_indexed, ord, "o_custkey",
                                                                                 lambda p: (((
                                                                                         p.o_orderdate >= ConstantExpr(
                                                                                     19931001))) * ((
                                                                                         p.o_orderdate < ConstantExpr(
                                                                                     19940101)))),
                                                                                 lambda indexedDictValue,
                                                                                        probeDictKey: DicConsExpr([(
                                                                                     probeDictKey.o_orderkey,
                                                                                     RecConsExpr(
                                                                                         [
                                                                                             (
                                                                                                 "c_custkey",
                                                                                                 indexedDictValue.c_custkey),
                                                                                             (
                                                                                                 "c_name",
                                                                                                 indexedDictValue.c_name),
                                                                                             (
                                                                                                 "c_acctbal",
                                                                                                 indexedDictValue.c_acctbal),
                                                                                             (
                                                                                                 "c_address",
                                                                                                 indexedDictValue.c_address),
                                                                                             (
                                                                                                 "c_phone",
                                                                                                 indexedDictValue.c_phone),
                                                                                             (
                                                                                                 "c_comment",
                                                                                                 indexedDictValue.c_comment),
                                                                                             (
                                                                                                 "n_name",
                                                                                                 na_indexed[
                                                                                                     indexedDictValue.c_nationkey].n_name)]))]),
                                                                                 True), LetExpr(li_probed,
                                                                                                JoinProbeBuilder(
                                                                                                    ord_probed, li,
                                                                                                    "l_orderkey",
                                                                                                    lambda p: (
                                                                                                            p.l_returnflag == r),
                                                                                                    lambda
                                                                                                        indexedDictValue,
                                                                                                        probeDictKey: DicConsExpr(
                                                                                                        [(RecConsExpr([(
                                                                                                            "c_custkey",
                                                                                                            indexedDictValue.c_custkey),
                                                                                                            (
                                                                                                                "c_name",
                                                                                                                indexedDictValue.c_name),
                                                                                                            (
                                                                                                                "c_acctbal",
                                                                                                                indexedDictValue.c_acctbal),
                                                                                                            (
                                                                                                                "n_name",
                                                                                                                indexedDictValue.n_name),
                                                                                                            (
                                                                                                                "c_address",
                                                                                                                indexedDictValue.c_address),
                                                                                                            (
                                                                                                                "c_phone",
                                                                                                                indexedDictValue.c_phone),
                                                                                                            (
                                                                                                                "c_comment",
                                                                                                                indexedDictValue.c_comment)]),
                                                                                                          (
                                                                                                                  probeDictKey.l_extendedprice * (
                                                                                                                  ConstantExpr(
                                                                                                                      1.0) - probeDictKey.l_discount)))]),
                                                                                                    False),
                                                                                                LetExpr(results,
                                                                                                        SumBuilder(
                                                                                                            lambda
                                                                                                                p: DicConsExpr(
                                                                                                                [(
                                                                                                                    RecConsExpr(
                                                                                                                        [
                                                                                                                            (
                                                                                                                                "c_custkey",
                                                                                                                                p[
                                                                                                                                    0].c_custkey),
                                                                                                                            (
                                                                                                                                "c_name",
                                                                                                                                p[
                                                                                                                                    0].c_name),
                                                                                                                            (
                                                                                                                                "revenue",
                                                                                                                                p[
                                                                                                                                    1]),
                                                                                                                            (
                                                                                                                                "c_acctbal",
                                                                                                                                p[
                                                                                                                                    0].c_acctbal),
                                                                                                                            (
                                                                                                                                "n_name",
                                                                                                                                p[
                                                                                                                                    0].n_name),
                                                                                                                            (
                                                                                                                                "c_address",
                                                                                                                                p[
                                                                                                                                    0].c_address),
                                                                                                                            (
                                                                                                                                "c_phone",
                                                                                                                                p[
                                                                                                                                    0].c_phone),
                                                                                                                            (
                                                                                                                                "c_comment",
                                                                                                                                p[
                                                                                                                                    0].c_comment)]),
                                                                                                                    ConstantExpr(
                                                                                                                        True))]),
                                                                                                            li_probed,
                                                                                                            True),
                                                                                                        LetExpr(VarExpr(
                                                                                                            "out"),
                                                                                                            results,
                                                                                                            ConstantExpr(
                                                                                                                True))))))))

    print(q10)


def q11():
    germany = VarExpr("germany")
    na_indexed = VarExpr("na_indexed")
    su_probed = VarExpr("su_probed")
    ps_probed = VarExpr("ps_probed")
    results = VarExpr("results")

    ps = VarExpr("db->ps_dataset")
    su = VarExpr("db->su_dataset")
    na = VarExpr("db->na_dataset")

    q11 = LetExpr(germany, ConstantExpr("GERMANY"),
                  LetExpr(na_indexed, JoinPartitionBuilder(na, "n_nationkey", lambda p: (p.n_name == germany), []),
                          LetExpr(su_probed,
                                  JoinProbeBuilder(na_indexed, su, "s_nationkey", lambda p: ConstantExpr(True),
                                                   lambda indexedDictValue, probeDictKey: DicConsExpr(
                                                       [(probeDictKey.s_suppkey, ConstantExpr(True))]), True),
                                  LetExpr(ps_probed,
                                          JoinProbeBuilder(su_probed, ps, "ps_suppkey", lambda p: ConstantExpr(True),
                                                           lambda indexedDictValue, probeDictKey: RecConsExpr([("A", ((
                                                                                                                              probeDictKey.ps_supplycost * probeDictKey.ps_availqty) * ConstantExpr(
                                                               0.0001))), ("B", DicConsExpr([(probeDictKey.ps_partkey, (
                                                                   probeDictKey.ps_supplycost * probeDictKey.ps_availqty))]))])),
                                          LetExpr(results, SumBuilder(lambda p: IfExpr((p[1] > ps_probed.A),
                                                                                       DicConsExpr([(RecConsExpr(
                                                                                           [("ps_partkey", p[0]),
                                                                                            ("value", p[1])]),
                                                                                                     ConstantExpr(
                                                                                                         True))]),
                                                                                       ConstantExpr(None)),
                                                                      ps_probed.B, ),
                                                  LetExpr(VarExpr("out"), results, ConstantExpr(True)))))))

    print(q11)


def q12():
    mail = VarExpr("mail")
    ship = VarExpr("ship")
    urgent = VarExpr("urgent")
    high = VarExpr("high")
    li_indexed = VarExpr("li_indexed")
    ord_probed = VarExpr("ord_probed")
    results = VarExpr("results")

    ord = VarExpr("db->ord_dataset")
    li = VarExpr("db->li_dataset")

    q12 = LetExpr(mail, ConstantExpr("MAIL"), LetExpr(ship, ConstantExpr("SHIP"),
                                                      LetExpr(urgent, ConstantExpr("1-URGENT"),
                                                              LetExpr(high, ConstantExpr("2-HIGH"), LetExpr(li_indexed,
                                                                                                            SumBuilder(
                                                                                                                lambda
                                                                                                                    p: IfExpr(
                                                                                                                    ((((
                                                                                                                           (
                                                                                                                                   p[
                                                                                                                                       0].l_shipmode == mail)) + (
                                                                                                                           (
                                                                                                                                   p[
                                                                                                                                       0].l_shipmode == ship)))) * (
                                                                                                                         (
                                                                                                                                     p[
                                                                                                                                         0].l_receiptdate >= ConstantExpr(
                                                                                                                                 19940101))) * (
                                                                                                                         (
                                                                                                                                     p[
                                                                                                                                         0].l_receiptdate < ConstantExpr(
                                                                                                                                 19950101))) * (
                                                                                                                         (
                                                                                                                                     p[
                                                                                                                                         0].l_shipdate <
                                                                                                                                     p[
                                                                                                                                         0].l_commitdate)) * (
                                                                                                                         (
                                                                                                                                     p[
                                                                                                                                         0].l_commitdate <
                                                                                                                                     p[
                                                                                                                                         0].l_receiptdate))),
                                                                                                                    DicConsExpr(
                                                                                                                        [
                                                                                                                            (
                                                                                                                                p[
                                                                                                                                    0].l_orderkey,
                                                                                                                                DicConsExpr(
                                                                                                                                    [
                                                                                                                                        (
                                                                                                                                            p[
                                                                                                                                                0].l_shipmode,
                                                                                                                                            ConstantExpr(
                                                                                                                                                1))]))]),
                                                                                                                    ConstantExpr(
                                                                                                                        None)),
                                                                                                                li,
                                                                                                                False),
                                                                                                            LetExpr(
                                                                                                                ord_probed,
                                                                                                                JoinProbeBuilder(
                                                                                                                    li_indexed,
                                                                                                                    ord,
                                                                                                                    "o_orderkey",
                                                                                                                    lambda
                                                                                                                        p: ConstantExpr(
                                                                                                                        True),
                                                                                                                    lambda
                                                                                                                        indexedDictValue,
                                                                                                                        probeDictKey: SumBuilder(
                                                                                                                        lambda
                                                                                                                            p: DicConsExpr(
                                                                                                                            [
                                                                                                                                (
                                                                                                                                    RecConsExpr(
                                                                                                                                        [
                                                                                                                                            (
                                                                                                                                                "l_shipmode",
                                                                                                                                                p[
                                                                                                                                                    0])]),
                                                                                                                                    RecConsExpr(
                                                                                                                                        [
                                                                                                                                            (
                                                                                                                                                "high_line_count",
                                                                                                                                                IfExpr(
                                                                                                                                                    (
                                                                                                                                                            (
                                                                                                                                                                (
                                                                                                                                                                        probeDictKey.o_orderpriority == urgent)) + (
                                                                                                                                                                (
                                                                                                                                                                        probeDictKey.o_orderpriority == high))),
                                                                                                                                                    p[
                                                                                                                                                        1],
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        0))),
                                                                                                                                            (
                                                                                                                                                "low_line_count",
                                                                                                                                                IfExpr(
                                                                                                                                                    (
                                                                                                                                                            (
                                                                                                                                                                (
                                                                                                                                                                        probeDictKey.o_orderpriority != urgent)) * (
                                                                                                                                                                (
                                                                                                                                                                        probeDictKey.o_orderpriority != high))),
                                                                                                                                                    p[
                                                                                                                                                        1],
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        0)))]))]),
                                                                                                                        indexedDictValue, ),
                                                                                                                    False),
                                                                                                                LetExpr(
                                                                                                                    results,
                                                                                                                    SumBuilder(
                                                                                                                        lambda
                                                                                                                            p: DicConsExpr(
                                                                                                                            [
                                                                                                                                (
                                                                                                                                    ConcatExpr(
                                                                                                                                        p[
                                                                                                                                            0],
                                                                                                                                        p[
                                                                                                                                            1]),
                                                                                                                                    ConstantExpr(
                                                                                                                                        True))]),
                                                                                                                        ord_probed,
                                                                                                                        True),
                                                                                                                    LetExpr(
                                                                                                                        VarExpr(
                                                                                                                            "out"),
                                                                                                                        results,
                                                                                                                        ConstantExpr(
                                                                                                                            True)))))))))

    return q12


def q13():
    special = VarExpr("special")
    requests = VarExpr("requests")
    ord_indexed = VarExpr("ord_indexed")
    customer_probed = VarExpr("customer_probed")
    results = VarExpr("results")

    cu = VarExpr("db->cu_dataset")
    ord = VarExpr("db->ord_dataset")

    q13 = LetExpr(special, ConstantExpr("special"), LetExpr(requests, ConstantExpr("requests"), LetExpr(ord_indexed,
                                                                                                        SumBuilder(
                                                                                                            lambda
                                                                                                                p: IfExpr(
                                                                                                                ((((
                                                                                                                        ExtFuncExpr(
                                                                                                                            ExtFuncSymbol.FirstIndex,
                                                                                                                            p[
                                                                                                                                0].o_comment,
                                                                                                                            special,
                                                                                                                            ConstantExpr(
                                                                                                                                "Nothing!")) != ConstantExpr(
                                                                                                                    -1) * (
                                                                                                                            ConstantExpr(
                                                                                                                                1)))) * (
                                                                                                                      (
                                                                                                                              ExtFuncExpr(
                                                                                                                                  ExtFuncSymbol.FirstIndex,
                                                                                                                                  p[
                                                                                                                                      0].o_comment,
                                                                                                                                  requests,
                                                                                                                                  ConstantExpr(
                                                                                                                                      "Nothing!")) > (
                                                                                                                                      ExtFuncExpr(
                                                                                                                                          ExtFuncSymbol.FirstIndex,
                                                                                                                                          p[
                                                                                                                                              0].o_comment,
                                                                                                                                          special,
                                                                                                                                          ConstantExpr(
                                                                                                                                              "Nothing!")) + ConstantExpr(
                                                                                                                                  6))))) == ConstantExpr(
                                                                                                                    False)),
                                                                                                                DicConsExpr(
                                                                                                                    [(p[
                                                                                                                          0].o_custkey,
                                                                                                                      ConstantExpr(
                                                                                                                          1))]),
                                                                                                                ConstantExpr(
                                                                                                                    None)),
                                                                                                            ord, ),
                                                                                                        LetExpr(
                                                                                                            customer_probed,
                                                                                                            SumBuilder(
                                                                                                                lambda
                                                                                                                    p: DicConsExpr(
                                                                                                                    [(
                                                                                                                        RecConsExpr(
                                                                                                                            [
                                                                                                                                (
                                                                                                                                    "c_count",
                                                                                                                                    IfExpr(
                                                                                                                                        (
                                                                                                                                                ord_indexed[
                                                                                                                                                    p[
                                                                                                                                                        0].c_custkey] != ConstantExpr(
                                                                                                                                            None)),
                                                                                                                                        ord_indexed[
                                                                                                                                            p[
                                                                                                                                                0].c_custkey],
                                                                                                                                        ConstantExpr(
                                                                                                                                            0)))]),
                                                                                                                        RecConsExpr(
                                                                                                                            [
                                                                                                                                (
                                                                                                                                    "custdist",
                                                                                                                                    ConstantExpr(
                                                                                                                                        1))]))]),
                                                                                                                cu, ),
                                                                                                            LetExpr(
                                                                                                                results,
                                                                                                                SumBuilder(
                                                                                                                    lambda
                                                                                                                        p: DicConsExpr(
                                                                                                                        [
                                                                                                                            (
                                                                                                                                ConcatExpr(
                                                                                                                                    p[
                                                                                                                                        0],
                                                                                                                                    p[
                                                                                                                                        1]),
                                                                                                                                ConstantExpr(
                                                                                                                                    True))]),
                                                                                                                    customer_probed,
                                                                                                                    True),
                                                                                                                LetExpr(
                                                                                                                    VarExpr(
                                                                                                                        "out"),
                                                                                                                    results,
                                                                                                                    ConstantExpr(
                                                                                                                        True)))))))

    print(q13)


def q14():
    promo = VarExpr("promo")
    pa_indexed = VarExpr("pa_indexed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    pa = VarExpr("db->pa_dataset")

    q14 = LetExpr(promo, ConstantExpr("PROMO"), LetExpr(pa_indexed, JoinPartitionBuilder(pa, "p_partkey",
                                                                                         lambda p: ExtFuncExpr(
                                                                                             ExtFuncSymbol.StartsWith,
                                                                                             p.p_type, promo,
                                                                                             ConstantExpr("Nothing!")),
                                                                                         []),
                                                        LetExpr(li_probed, SumBuilder(lambda p: IfExpr((((
                                                                p[0].l_shipdate >= ConstantExpr(19950901))) * ((
                                                                p[0].l_shipdate < ConstantExpr(19951001)))),
                                                                                                       RecConsExpr([
                                                                                                           (
                                                                                                               "A",
                                                                                                               IfExpr(
                                                                                                                   (
                                                                                                                           pa_indexed[
                                                                                                                               p[
                                                                                                                                   0].l_partkey] != ConstantExpr(
                                                                                                                       None)),
                                                                                                                   (
                                                                                                                           p[
                                                                                                                               0].l_extendedprice * (
                                                                                                                                   ConstantExpr(
                                                                                                                                       1.0) -
                                                                                                                                   p[
                                                                                                                                       0].l_discount)),
                                                                                                                   ConstantExpr(
                                                                                                                       0.0))),
                                                                                                           (
                                                                                                               "B",
                                                                                                               (
                                                                                                                       p[
                                                                                                                           0].l_extendedprice * (
                                                                                                                               ConstantExpr(
                                                                                                                                   1.0) -
                                                                                                                               p[
                                                                                                                                   0].l_discount)))]),
                                                                                                       ConstantExpr(
                                                                                                           None)),
                                                                                      li, ), LetExpr(results, (
                                                                (ConstantExpr(100.0) * li_probed.A) / li_probed.B),
                                                                                                     LetExpr(
                                                                                                         VarExpr("out"),
                                                                                                         results,
                                                                                                         ConstantExpr(
                                                                                                             True))))))
    print(q14)


def q15():
    li_aggr = VarExpr("li_aggr")
    max_revenue = VarExpr("max_revenue")
    su_indexed = VarExpr("su_indexed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    su = VarExpr("db->su_dataset")

    q15 = LetExpr(li_aggr, SumBuilder(
        lambda p: IfExpr((((p[0].l_shipdate >= ConstantExpr(19960101))) * ((p[0].l_shipdate < ConstantExpr(19960401)))),
                         DicConsExpr(
                             [(p[0].l_suppkey, (p[0].l_extendedprice * (ConstantExpr(1.0) - p[0].l_discount)))]),
                         ConstantExpr(None)), li, ), LetExpr(max_revenue, ConstantExpr(1772627.2087),
                                                             LetExpr(su_indexed, JoinPartitionBuilder(su, "s_suppkey",
                                                                                                      lambda
                                                                                                          p: ConstantExpr(
                                                                                                          True),
                                                                                                      ["s_name",
                                                                                                       "s_address",
                                                                                                       "s_phone"]),
                                                                     LetExpr(results, SumBuilder(
                                                                         lambda p: IfExpr((p[1] == max_revenue),
                                                                                          DicConsExpr([(RecConsExpr(
                                                                                              [("s_suppkey", p[0]), (
                                                                                                  "s_name",
                                                                                                  su_indexed[
                                                                                                      p[0]].s_name),
                                                                                               ("s_address", su_indexed[
                                                                                                   p[0]].s_address), (
                                                                                                   "s_phone",
                                                                                                   su_indexed[
                                                                                                       p[0]].s_phone), (
                                                                                                   "total_revenue",
                                                                                                   p[1])]),
                                                                                                        ConstantExpr(
                                                                                                            True))]),
                                                                                          ConstantExpr(None)), li_aggr,
                                                                         True), LetExpr(VarExpr("out"), results,
                                                                                        ConstantExpr(True))))))
    print(q15)


def q16():
    brand45 = VarExpr("brand45")
    medpol = VarExpr("medpol")
    Customer = VarExpr("Customer")
    complaints = VarExpr("complaints")
    part_indexed = VarExpr("part_indexed")
    su_indexed = VarExpr("su_indexed")
    partsupp_probe = VarExpr("partsupp_probe")
    results = VarExpr("results")

    ps = VarExpr("db->ps_dataset")
    pa = VarExpr("db->pa_dataset")
    su = VarExpr("db->su_dataset")

    q16 = LetExpr(brand45, ConstantExpr("Brand#45"), LetExpr(medpol, ConstantExpr("MEDIUM POLISHED"),
                                                             LetExpr(Customer, ConstantExpr("Customer"),
                                                                     LetExpr(complaints, ConstantExpr("Complaints"),
                                                                             LetExpr(part_indexed,
                                                                                     JoinPartitionBuilder(pa,
                                                                                                          "p_partkey",
                                                                                                          lambda p: (((
                                                                                                                  p.p_brand != brand45)) * (
                                                                                                                         (
                                                                                                                                 ExtFuncExpr(
                                                                                                                                     ExtFuncSymbol.StartsWith,
                                                                                                                                     p.p_type,
                                                                                                                                     medpol,
                                                                                                                                     ConstantExpr(
                                                                                                                                         "Nothing!")) == ConstantExpr(
                                                                                                                             False))) * (
                                                                                                                         (
                                                                                                                                     (
                                                                                                                                     (
                                                                                                                                             p.p_size == ConstantExpr(
                                                                                                                                         49))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             14))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             23))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             45))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             19))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             3))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             36))) + (
                                                                                                                                         (
                                                                                                                                                 p.p_size == ConstantExpr(
                                                                                                                                             9)))))),
                                                                                                          ["p_brand",
                                                                                                           "p_type",
                                                                                                           "p_size"]),
                                                                                     LetExpr(su_indexed,
                                                                                             JoinPartitionBuilder(su,
                                                                                                                  "s_suppkey",
                                                                                                                  lambda
                                                                                                                      p: (
                                                                                                                          (
                                                                                                                              (
                                                                                                                                      ExtFuncExpr(
                                                                                                                                          ExtFuncSymbol.FirstIndex,
                                                                                                                                          p.s_comment,
                                                                                                                                          Customer,
                                                                                                                                          ConstantExpr(
                                                                                                                                              "Nothing!")) != ConstantExpr(
                                                                                                                                  -1) * (
                                                                                                                                          ConstantExpr(
                                                                                                                                              1)))) * (
                                                                                                                              (
                                                                                                                                      ExtFuncExpr(
                                                                                                                                          ExtFuncSymbol.FirstIndex,
                                                                                                                                          p.s_comment,
                                                                                                                                          complaints,
                                                                                                                                          ConstantExpr(
                                                                                                                                              "Nothing!")) > (
                                                                                                                                              ExtFuncExpr(
                                                                                                                                                  ExtFuncSymbol.FirstIndex,
                                                                                                                                                  p.s_comment,
                                                                                                                                                  Customer,
                                                                                                                                                  ConstantExpr(
                                                                                                                                                      "Nothing!")) + ConstantExpr(
                                                                                                                                          7))))),
                                                                                                                  []),
                                                                                             LetExpr(partsupp_probe,
                                                                                                     JoinProbeBuilder(
                                                                                                         part_indexed,
                                                                                                         ps,
                                                                                                         "ps_partkey",
                                                                                                         lambda
                                                                                                             p: ConstantExpr(
                                                                                                             True),
                                                                                                         lambda
                                                                                                             indexedDictValue,
                                                                                                             probeDictKey: IfExpr(
                                                                                                             (
                                                                                                                     su_indexed[
                                                                                                                         probeDictKey.ps_suppkey] == ConstantExpr(
                                                                                                                 None)),
                                                                                                             DicConsExpr(
                                                                                                                 [(
                                                                                                                     RecConsExpr(
                                                                                                                         [
                                                                                                                             (
                                                                                                                                 "p_brand",
                                                                                                                                 indexedDictValue.p_brand),
                                                                                                                             (
                                                                                                                                 "p_type",
                                                                                                                                 indexedDictValue.p_type),
                                                                                                                             (
                                                                                                                                 "p_size",
                                                                                                                                 indexedDictValue.p_size)]),
                                                                                                                     DicConsExpr(
                                                                                                                         [
                                                                                                                             (
                                                                                                                                 probeDictKey.ps_suppkey,
                                                                                                                                 ConstantExpr(
                                                                                                                                     True))]))]),
                                                                                                             ConstantExpr(
                                                                                                                 None)),
                                                                                                         False),
                                                                                                     LetExpr(results,
                                                                                                             SumBuilder(
                                                                                                                 lambda
                                                                                                                     p: DicConsExpr(
                                                                                                                     [(
                                                                                                                         ConcatExpr(
                                                                                                                             p[
                                                                                                                                 0],
                                                                                                                             RecConsExpr(
                                                                                                                                 [
                                                                                                                                     (
                                                                                                                                         "supplier_cnt",
                                                                                                                                         ExtFuncExpr(
                                                                                                                                             ExtFuncSymbol.DictSize,
                                                                                                                                             p[
                                                                                                                                                 1],
                                                                                                                                             ConstantExpr(
                                                                                                                                                 "Nothing!"),
                                                                                                                                             ConstantExpr(
                                                                                                                                                 "Nothing!")))])),
                                                                                                                         ConstantExpr(
                                                                                                                             True))]),
                                                                                                                 partsupp_probe, ),
                                                                                                             LetExpr(
                                                                                                                 VarExpr(
                                                                                                                     "out"),
                                                                                                                 results,
                                                                                                                 ConstantExpr(
                                                                                                                     True))))))))))

    print(q16)


def q18():
    li_aggregated = VarExpr("li_aggregated")
    li_filtered = VarExpr("li_filtered")
    cu_indexed = VarExpr("cu_indexed")
    order_probed = VarExpr("order_probed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    cu = VarExpr("db->cu_dataset")
    ord = VarExpr("db->ord_dataset")

    q18 = LetExpr(li_aggregated, SumBuilder(lambda b: DicConsExpr([(b[0].l_orderkey, b[0].l_quantity)]), li, False),
                  LetExpr(li_filtered, SumBuilder(
                      lambda z: IfExpr((z[1] > ConstantExpr(300)), DicConsExpr([(z[0], ConstantExpr(True))]),
                                       ConstantExpr(None)), li_aggregated, True), LetExpr(cu_indexed,
                                                                                          JoinPartitionBuilder(cu,
                                                                                                               "c_custkey",
                                                                                                               lambda
                                                                                                                   p: ConstantExpr(
                                                                                                                   True),
                                                                                                               [
                                                                                                                   "c_name"]),
                                                                                          LetExpr(order_probed,
                                                                                                  JoinProbeBuilder(
                                                                                                      cu_indexed, ord,
                                                                                                      "o_custkey",
                                                                                                      lambda p: (
                                                                                                              li_filtered[
                                                                                                                  p.o_orderkey] != ConstantExpr(
                                                                                                          None)),
                                                                                                      lambda
                                                                                                          indexedDictValue,
                                                                                                          probeDictKey: DicConsExpr(
                                                                                                          [(
                                                                                                              probeDictKey.o_orderkey,
                                                                                                              RecConsExpr(
                                                                                                                  [
                                                                                                                      (
                                                                                                                          "c_name",
                                                                                                                          indexedDictValue.c_name),
                                                                                                                      (
                                                                                                                          "o_custkey",
                                                                                                                          probeDictKey.o_custkey),
                                                                                                                      (
                                                                                                                          "o_orderkey",
                                                                                                                          probeDictKey.o_orderkey),
                                                                                                                      (
                                                                                                                          "o_orderdate",
                                                                                                                          probeDictKey.o_orderdate),
                                                                                                                      (
                                                                                                                          "o_totalprice",
                                                                                                                          probeDictKey.o_totalprice)]))]),
                                                                                                      True),
                                                                                                  LetExpr(li_probed,
                                                                                                          JoinProbeBuilder(
                                                                                                              order_probed,
                                                                                                              li,
                                                                                                              "l_orderkey",
                                                                                                              lambda
                                                                                                                  p: ConstantExpr(
                                                                                                                  True),
                                                                                                              lambda
                                                                                                                  indexedDictValue,
                                                                                                                  probeDictKey: DicConsExpr(
                                                                                                                  [(
                                                                                                                      RecConsExpr(
                                                                                                                          [
                                                                                                                              (
                                                                                                                                  "c_name",
                                                                                                                                  indexedDictValue.c_name),
                                                                                                                              (
                                                                                                                                  "o_custkey",
                                                                                                                                  indexedDictValue.o_custkey),
                                                                                                                              (
                                                                                                                                  "o_orderkey",
                                                                                                                                  indexedDictValue.o_orderkey),
                                                                                                                              (
                                                                                                                                  "o_orderdate",
                                                                                                                                  indexedDictValue.o_orderdate),
                                                                                                                              (
                                                                                                                                  "o_totalprice",
                                                                                                                                  indexedDictValue.o_totalprice)]),
                                                                                                                      RecConsExpr(
                                                                                                                          [
                                                                                                                              (
                                                                                                                                  "quantitysum",
                                                                                                                                  probeDictKey.l_quantity)]))])),
                                                                                                          LetExpr(
                                                                                                              results,
                                                                                                              SumBuilder(
                                                                                                                  lambda
                                                                                                                      p: DicConsExpr(
                                                                                                                      [(
                                                                                                                          ConcatExpr(
                                                                                                                              p[
                                                                                                                                  0],
                                                                                                                              p[
                                                                                                                                  1]),
                                                                                                                          ConstantExpr(
                                                                                                                              True))]),
                                                                                                                  li_probed,
                                                                                                                  True),
                                                                                                              LetExpr(
                                                                                                                  VarExpr(
                                                                                                                      "out"),
                                                                                                                  results,
                                                                                                                  ConstantExpr(
                                                                                                                      True))))))))
    print(q18)


def q19():
    brand12 = VarExpr("brand12")
    brand23 = VarExpr("brand23")
    brand34 = VarExpr("brand34")
    smcase = VarExpr("smcase")
    smbox = VarExpr("smbox")
    smpack = VarExpr("smpack")
    smpkg = VarExpr("smpkg")
    mdbag = VarExpr("mdbag")
    mdbox = VarExpr("mdbox")
    mdpack = VarExpr("mdpack")
    mdpkg = VarExpr("mdpkg")
    lgcase = VarExpr("lgcase")
    lgbox = VarExpr("lgbox")
    lgpack = VarExpr("lgpack")
    lgpkg = VarExpr("lgpkg")
    air = VarExpr("air")
    airreg = VarExpr("airreg")
    deliverinperson = VarExpr("deliverinperson")
    pa_indexed = VarExpr("pa_indexed")
    li_probed = VarExpr("li_probed")
    results = VarExpr("results")

    li = VarExpr("db->li_dataset")
    pa = VarExpr("db->pa_dataset")
    q19 = LetExpr(brand12, ConstantExpr("Brand#12"), LetExpr(brand23, ConstantExpr("Brand#23"),
                                                             LetExpr(brand34, ConstantExpr("Brand#34"),
                                                                     LetExpr(smcase, ConstantExpr("SM CASE"),
                                                                             LetExpr(smbox, ConstantExpr("SM BOX"),
                                                                                     LetExpr(smpack,
                                                                                             ConstantExpr("SM PACK"),
                                                                                             LetExpr(smpkg,
                                                                                                     ConstantExpr(
                                                                                                         "SM PKG"),
                                                                                                     LetExpr(mdbag,
                                                                                                             ConstantExpr(
                                                                                                                 "MED BAG"),
                                                                                                             LetExpr(
                                                                                                                 mdbox,
                                                                                                                 ConstantExpr(
                                                                                                                     "MED BOX"),
                                                                                                                 LetExpr(
                                                                                                                     mdpack,
                                                                                                                     ConstantExpr(
                                                                                                                         "MED PACK"),
                                                                                                                     LetExpr(
                                                                                                                         mdpkg,
                                                                                                                         ConstantExpr(
                                                                                                                             "MED PKG"),
                                                                                                                         LetExpr(
                                                                                                                             lgcase,
                                                                                                                             ConstantExpr(
                                                                                                                                 "LG CASE"),
                                                                                                                             LetExpr(
                                                                                                                                 lgbox,
                                                                                                                                 ConstantExpr(
                                                                                                                                     "LG BOX"),
                                                                                                                                 LetExpr(
                                                                                                                                     lgpack,
                                                                                                                                     ConstantExpr(
                                                                                                                                         "LG PACK"),
                                                                                                                                     LetExpr(
                                                                                                                                         lgpkg,
                                                                                                                                         ConstantExpr(
                                                                                                                                             "LG PKG"),
                                                                                                                                         LetExpr(
                                                                                                                                             air,
                                                                                                                                             ConstantExpr(
                                                                                                                                                 "AIR"),
                                                                                                                                             LetExpr(
                                                                                                                                                 airreg,
                                                                                                                                                 ConstantExpr(
                                                                                                                                                     "AIR REG"),
                                                                                                                                                 LetExpr(
                                                                                                                                                     deliverinperson,
                                                                                                                                                     ConstantExpr(
                                                                                                                                                         "DELIVER IN PERSON"),
                                                                                                                                                     LetExpr(
                                                                                                                                                         pa_indexed,
                                                                                                                                                         JoinPartitionBuilder(
                                                                                                                                                             pa,
                                                                                                                                                             "p_partkey",
                                                                                                                                                             lambda
                                                                                                                                                                 p: (
                                                                                                                                                                     (
                                                                                                                                                                         (
                                                                                                                                                                                 (
                                                                                                                                                                                     (
                                                                                                                                                                                             p.p_brand == brand12)) * (
                                                                                                                                                                                     (
                                                                                                                                                                                             (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == smcase)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == smbox)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == smpack)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == smpkg)))) * (
                                                                                                                                                                                     (
                                                                                                                                                                                             (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_size >= ConstantExpr(
                                                                                                                                                                                                     1))) * (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_size <= ConstantExpr(
                                                                                                                                                                                                     5))))))) + (
                                                                                                                                                                         (
                                                                                                                                                                                 (
                                                                                                                                                                                     (
                                                                                                                                                                                             p.p_brand == brand23)) * (
                                                                                                                                                                                     (
                                                                                                                                                                                             (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == mdbag)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == mdbox)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == mdpack)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == mdpkg)))) * (
                                                                                                                                                                                     (
                                                                                                                                                                                             (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_size >= ConstantExpr(
                                                                                                                                                                                                     1))) * (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_size <= ConstantExpr(
                                                                                                                                                                                                     10))))))) + (
                                                                                                                                                                         (
                                                                                                                                                                                 (
                                                                                                                                                                                     (
                                                                                                                                                                                             p.p_brand == brand34)) * (
                                                                                                                                                                                     (
                                                                                                                                                                                             (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == lgcase)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == lgbox)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == lgpack)) + (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_container == lgpkg)))) * (
                                                                                                                                                                                     (
                                                                                                                                                                                             (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_size >= ConstantExpr(
                                                                                                                                                                                                     1))) * (
                                                                                                                                                                                                 (
                                                                                                                                                                                                         p.p_size <= ConstantExpr(
                                                                                                                                                                                                     15)))))))),
                                                                                                                                                             [
                                                                                                                                                                 "p_brand",
                                                                                                                                                                 "p_size",
                                                                                                                                                                 "p_container"]),
                                                                                                                                                         LetExpr(
                                                                                                                                                             li_probed,
                                                                                                                                                             JoinProbeBuilder(
                                                                                                                                                                 pa_indexed,
                                                                                                                                                                 li,
                                                                                                                                                                 "l_partkey",
                                                                                                                                                                 lambda
                                                                                                                                                                     p: (
                                                                                                                                                                         (
                                                                                                                                                                             (
                                                                                                                                                                                     p.l_shipinstruct == deliverinperson)) * (
                                                                                                                                                                             (
                                                                                                                                                                                     (
                                                                                                                                                                                         (
                                                                                                                                                                                                 p.l_shipmode == air)) + (
                                                                                                                                                                                         (
                                                                                                                                                                                                 p.l_shipmode == airreg))))),
                                                                                                                                                                 lambda
                                                                                                                                                                     indexedDictValue,
                                                                                                                                                                     probeDictKey: IfExpr(
                                                                                                                                                                     (
                                                                                                                                                                             (
                                                                                                                                                                                 (
                                                                                                                                                                                         (
                                                                                                                                                                                             (
                                                                                                                                                                                                     indexedDictValue.p_brand == brand12)) * (
                                                                                                                                                                                             (
                                                                                                                                                                                                     (
                                                                                                                                                                                                         (
                                                                                                                                                                                                                 probeDictKey.l_quantity >= ConstantExpr(
                                                                                                                                                                                                             1))) * (
                                                                                                                                                                                                         (
                                                                                                                                                                                                                 probeDictKey.l_quantity <= ConstantExpr(
                                                                                                                                                                                                             11))))))) + (
                                                                                                                                                                                 (
                                                                                                                                                                                         (
                                                                                                                                                                                             (
                                                                                                                                                                                                     indexedDictValue.p_brand == brand23)) * (
                                                                                                                                                                                             (
                                                                                                                                                                                                     (
                                                                                                                                                                                                         (
                                                                                                                                                                                                                 probeDictKey.l_quantity >= ConstantExpr(
                                                                                                                                                                                                             10))) * (
                                                                                                                                                                                                         (
                                                                                                                                                                                                                 probeDictKey.l_quantity <= ConstantExpr(
                                                                                                                                                                                                             20))))))) + (
                                                                                                                                                                                 (
                                                                                                                                                                                         (
                                                                                                                                                                                             (
                                                                                                                                                                                                     indexedDictValue.p_brand == brand34)) * (
                                                                                                                                                                                             (
                                                                                                                                                                                                     (
                                                                                                                                                                                                         (
                                                                                                                                                                                                                 probeDictKey.l_quantity >= ConstantExpr(
                                                                                                                                                                                                             20))) * (
                                                                                                                                                                                                         (
                                                                                                                                                                                                                 probeDictKey.l_quantity <= ConstantExpr(
                                                                                                                                                                                                             30)))))))),
                                                                                                                                                                     (
                                                                                                                                                                             probeDictKey.l_extendedprice * (
                                                                                                                                                                             ConstantExpr(
                                                                                                                                                                                 1.0) - probeDictKey.l_discount)),
                                                                                                                                                                     ConstantExpr(
                                                                                                                                                                         None))),
                                                                                                                                                             LetExpr(
                                                                                                                                                                 results,
                                                                                                                                                                 DicConsExpr(
                                                                                                                                                                     [
                                                                                                                                                                         (
                                                                                                                                                                             RecConsExpr(
                                                                                                                                                                                 [
                                                                                                                                                                                     (
                                                                                                                                                                                         "revenue",
                                                                                                                                                                                         li_probed)]),
                                                                                                                                                                             ConstantExpr(
                                                                                                                                                                                 True))]),
                                                                                                                                                                 LetExpr(
                                                                                                                                                                     VarExpr(
                                                                                                                                                                         "out"),
                                                                                                                                                                     results,
                                                                                                                                                                     ConstantExpr(
                                                                                                                                                                         True)))))))))))))))))))))))
    print(q19)


def q21():
    saudi = VarExpr("saudi")
    f = VarExpr("f")
    nation_indexed = VarExpr("nation_indexed")
    su_probed = VarExpr("su_probed")
    ord_indexed = VarExpr("ord_indexed")
    l2_indexed = VarExpr("l2_indexed")
    l3_indexed = VarExpr("l3_indexed")
    l1_probed = VarExpr("l1_probed")
    results = VarExpr("results")

    su = VarExpr("db->su_dataset")
    li = VarExpr("db->li_dataset")
    ord = VarExpr("db->ord_dataset")
    na = VarExpr("db->na_dataset")

    q21 = LetExpr(saudi, ConstantExpr("SAUDI ARABIA"), LetExpr(f, ConstantExpr("F"), LetExpr(nation_indexed,
                                                                                             JoinPartitionBuilder(na,
                                                                                                                  "n_nationkey",
                                                                                                                  lambda
                                                                                                                      p: (
                                                                                                                          p.n_name == saudi),
                                                                                                                  []),
                                                                                             LetExpr(su_probed,
                                                                                                     JoinProbeBuilder(
                                                                                                         nation_indexed,
                                                                                                         su,
                                                                                                         "s_nationkey",
                                                                                                         lambda
                                                                                                             p: ConstantExpr(
                                                                                                             True),
                                                                                                         lambda
                                                                                                             indexedDictValue,
                                                                                                             probeDictKey: DicConsExpr(
                                                                                                             [(
                                                                                                                 probeDictKey.s_suppkey,
                                                                                                                 probeDictKey.s_name)]),
                                                                                                         True), LetExpr(
                                                                                                     ord_indexed,
                                                                                                     SumBuilder(lambda
                                                                                                                    p: IfExpr(
                                                                                                         (p[
                                                                                                              0].o_orderstatus == f),
                                                                                                         DicConsExpr([(
                                                                                                             p[
                                                                                                                 0].o_orderkey,
                                                                                                             ConstantExpr(
                                                                                                                 True))]),
                                                                                                         ConstantExpr(
                                                                                                             None)),
                                                                                                                ord,
                                                                                                                True,
                                                                                                                "dense_array(6000000)"),
                                                                                                     LetExpr(l2_indexed,
                                                                                                             SumBuilder(
                                                                                                                 lambda
                                                                                                                     p: DicConsExpr(
                                                                                                                     [(
                                                                                                                         p[
                                                                                                                             0].l_orderkey,
                                                                                                                         VecConsExpr(
                                                                                                                             [
                                                                                                                                 p[
                                                                                                                                     0].l_suppkey]))]),
                                                                                                                 li,
                                                                                                                 False,
                                                                                                                 "dense_array(6000000)"),
                                                                                                             LetExpr(
                                                                                                                 l3_indexed,
                                                                                                                 SumBuilder(
                                                                                                                     lambda
                                                                                                                         p: IfExpr(
                                                                                                                         (
                                                                                                                                 p[
                                                                                                                                     0].l_receiptdate >
                                                                                                                                 p[
                                                                                                                                     0].l_commitdate),
                                                                                                                         DicConsExpr(
                                                                                                                             [
                                                                                                                                 (
                                                                                                                                     p[
                                                                                                                                         0].l_orderkey,
                                                                                                                                     VecConsExpr(
                                                                                                                                         [
                                                                                                                                             p[
                                                                                                                                                 0].l_suppkey]))]),
                                                                                                                         ConstantExpr(
                                                                                                                             None)),
                                                                                                                     li,
                                                                                                                     False,
                                                                                                                     "dense_array(6000000)"),
                                                                                                                 LetExpr(
                                                                                                                     l1_probed,
                                                                                                                     SumBuilder(
                                                                                                                         lambda
                                                                                                                             p: IfExpr(
                                                                                                                             (
                                                                                                                                     (
                                                                                                                                         (
                                                                                                                                                 p[
                                                                                                                                                     0].l_receiptdate >
                                                                                                                                                 p[
                                                                                                                                                     0].l_commitdate)) * (
                                                                                                                                         (
                                                                                                                                                 su_probed[
                                                                                                                                                     p[
                                                                                                                                                         0].l_suppkey] != ConstantExpr(
                                                                                                                                             None))) * (
                                                                                                                                         (
                                                                                                                                                 ord_indexed[
                                                                                                                                                     p[
                                                                                                                                                         0].l_orderkey] != ConstantExpr(
                                                                                                                                             None))) * (
                                                                                                                                         (
                                                                                                                                                 ExtFuncExpr(
                                                                                                                                                     ExtFuncSymbol.DictSize,
                                                                                                                                                     l2_indexed[
                                                                                                                                                         p[
                                                                                                                                                             0].l_orderkey],
                                                                                                                                                     ConstantExpr(
                                                                                                                                                         "Nothing!"),
                                                                                                                                                     ConstantExpr(
                                                                                                                                                         "Nothing!")) > ConstantExpr(
                                                                                                                                             1))) * (
                                                                                                                                         (
                                                                                                                                                 (
                                                                                                                                                         (
                                                                                                                                                             (
                                                                                                                                                                     ExtFuncExpr(
                                                                                                                                                                         ExtFuncSymbol.DictSize,
                                                                                                                                                                         l3_indexed[
                                                                                                                                                                             p[
                                                                                                                                                                                 0].l_orderkey],
                                                                                                                                                                         ConstantExpr(
                                                                                                                                                                             "Nothing!"),
                                                                                                                                                                         ConstantExpr(
                                                                                                                                                                             "Nothing!")) > ConstantExpr(
                                                                                                                                                                 0))) * (
                                                                                                                                                             (
                                                                                                                                                                     ExtFuncExpr(
                                                                                                                                                                         ExtFuncSymbol.DictSize,
                                                                                                                                                                         l3_indexed[
                                                                                                                                                                             p[
                                                                                                                                                                                 0].l_orderkey],
                                                                                                                                                                         ConstantExpr(
                                                                                                                                                                             "Nothing!"),
                                                                                                                                                                         ConstantExpr(
                                                                                                                                                                             "Nothing!")) > ConstantExpr(
                                                                                                                                                                 1)))) == ConstantExpr(
                                                                                                                                             False)))),
                                                                                                                             DicConsExpr(
                                                                                                                                 [
                                                                                                                                     (
                                                                                                                                         RecConsExpr(
                                                                                                                                             [
                                                                                                                                                 (
                                                                                                                                                     "s_name",
                                                                                                                                                     su_probed[
                                                                                                                                                         p[
                                                                                                                                                             0].l_suppkey])]),
                                                                                                                                         RecConsExpr(
                                                                                                                                             [
                                                                                                                                                 (
                                                                                                                                                     "numwait",
                                                                                                                                                     ConstantExpr(
                                                                                                                                                         1))]))]),
                                                                                                                             ConstantExpr(
                                                                                                                                 None)),
                                                                                                                         li,
                                                                                                                         False),
                                                                                                                     LetExpr(
                                                                                                                         results,
                                                                                                                         SumBuilder(
                                                                                                                             lambda
                                                                                                                                 p: DicConsExpr(
                                                                                                                                 [
                                                                                                                                     (
                                                                                                                                         ConcatExpr(
                                                                                                                                             p[
                                                                                                                                                 0],
                                                                                                                                             p[
                                                                                                                                                 1]),
                                                                                                                                         ConstantExpr(
                                                                                                                                             True))]),
                                                                                                                             l1_probed,
                                                                                                                             True),
                                                                                                                         LetExpr(
                                                                                                                             VarExpr(
                                                                                                                                 "out"),
                                                                                                                             results,
                                                                                                                             ConstantExpr(
                                                                                                                                 True)))))))))))

    # print(q21)

    return q21


if __name__ == '__main__':
    # q1()
    # q2()
    # q3()
    # q4()
    # q5()
    # q6()
    # q7()
    # q8()
    # q9()
    # q10()
    # q14()
    # q15()
    # q16()
    # q18()
    # q19()
    q21()

    # print(GenerateSDQLPYCode(q12(), {}))

    print(q12())
