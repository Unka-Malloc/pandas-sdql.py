from pysdql.core.dtypes.sdql_ir import VarExpr, LetExpr, ConstantExpr, JoinPartitionBuilder, DicConsExpr, RecConsExpr, \
    JoinProbeBuilder, SumBuilder, ConcatExpr, IfExpr

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
                                                                                                                     [(
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


if __name__ == '__main__':
    # q1()
    # q3()
    q4()
    # q6()
    # q10()
    # q19()
