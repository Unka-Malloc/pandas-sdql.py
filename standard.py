def tpch_1(li):
    li_filt = li[(li.l_shipdate <= "1998-09-02")]

    li_filt["disc_price"] = li_filt.l_extendedprice * (1 - li_filt.l_discount)
    li_filt["charge"] = li_filt.l_extendedprice * (1 - li_filt.l_discount) * (1 + li_filt.l_tax)

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"]) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count"))
    return result


def func_tpch_1(li):
    lineitem_probed = li.sum(lambda p:
                             {
                                 record({"l_returnflag": p[0].l_returnflag, "l_linestatus": p[0].l_linestatus}):
                                     record({"sum_qty": p[0].l_quantity, "sum_base_price": p[0].l_extendedprice,
                                             "sum_disc_price": (p[0].l_extendedprice * (1.0 - p[0].l_discount)),
                                             "sum_charge": ((p[0].l_extendedprice * (1.0 - p[0].l_discount)) * (
                                                     1.0 + p[0].l_tax)), "count_order": 1})
                             }
                             if
                             p[0].l_shipdate <= 19980902
                             else
                             None,
                             True
                             )

    results = lineitem_probed.sum(lambda p: {p[0].concat(p[1]): True}, False)

    q1 = LetExpr(lineitem_probed,
                 SumBuilder(lambda p: IfExpr((p[0].l_shipdate <= ConstantExpr(19980902)), DicConsExpr([(RecConsExpr(
                     [("l_returnflag", p[0].l_returnflag), ("l_linestatus", p[0].l_linestatus)]), RecConsExpr(
                     [("sum_qty", p[0].l_quantity), ("sum_base_price", p[0].l_extendedprice),
                      ("sum_disc_price", (p[0].l_extendedprice * (ConstantExpr(1.0) - p[0].l_discount))), ("sum_charge",
                                                                                                           ((p[
                                                                                                                 0].l_extendedprice * (
                                                                                                                     ConstantExpr(
                                                                                                                         1.0) -
                                                                                                                     p[
                                                                                                                         0].l_discount)) * (
                                                                                                                    ConstantExpr(
                                                                                                                        1.0) +
                                                                                                                    p[
                                                                                                                        0].l_tax))),
                      ("count_order", ConstantExpr(1))]))]), ConstantExpr(None)),
                            li,
                            False),
                 LetExpr(results,
                         SumBuilder(lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]),
                                    lineitem_probed,
                                    True),
                         LetExpr(VarExpr("out"), results, ConstantExpr(True))))

    q3 = LetExpr(building,
                 ConstantExpr("BUILDING"),
                 LetExpr(customer_indexed,
                         JoinPartitionBuilder(cu,
                                              "c_custkey",
                                              lambda p: (p.c_mktsegment == building),
                                              []),
                         LetExpr(order_probed,
                                 JoinProbeBuilder(customer_indexed, ord,
                                                  "o_custkey",
                                                  lambda p: (p.o_orderdate < ConstantExpr(19950315)),
                                                  lambda indexedDictValue, probeDictKey:
                                                  DicConsExpr([(probeDictKey.o_orderkey, RecConsExpr(
                                                      [("o_orderdate", probeDictKey.o_orderdate),
                                                       ("o_shippriority", probeDictKey.o_shippriority)]))]),
                                                  True), LetExpr(lineitem_probed,
                                                                 JoinProbeBuilder(
                                                                     order_probed,
                                                                     li,
                                                                     "l_orderkey",
                                                                     lambda p: (
                                                                             p.l_shipdate > ConstantExpr(
                                                                         19950315)),
                                                                     lambda
                                                                         indexedDictValue,
                                                                         probeDictKey: DicConsExpr(
                                                                         [(
                                                                             RecConsExpr(
                                                                                 [(
                                                                                     "l_orderkey",
                                                                                     probeDictKey.l_orderkey),
                                                                                     (
                                                                                         "o_orderdate",
                                                                                         indexedDictValue.o_orderdate),
                                                                                     (
                                                                                         "o_shippriority",
                                                                                         indexedDictValue.o_shippriority)]),
                                                                             RecConsExpr(
                                                                                 [(
                                                                                     "revenue",
                                                                                     (
                                                                                             probeDictKey.l_extendedprice * (
                                                                                             ConstantExpr(
                                                                                                 1.0) - probeDictKey.l_discount)))]))])),
                                                                 LetExpr(results,
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
                                                                             lineitem_probed,
                                                                             True),
                                                                         LetExpr(
                                                                             VarExpr(
                                                                                 "out"),
                                                                             results,
                                                                             ConstantExpr(
                                                                                 True)))))))
