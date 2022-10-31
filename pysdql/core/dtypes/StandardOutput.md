# Q1
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(lineitem_probed, 
        SumBuilder(lambda p: IfExpr((p[0].l_shipdate <= ConstantExpr(19980902)), 
                                    DicConsExpr([(RecConsExpr([("l_returnflag", p[0].l_returnflag), 
                                                               ("l_linestatus", p[0].l_linestatus)
                                                               ]), 
                                                  RecConsExpr([("sum_qty", p[0].l_quantity), 
                                                               ("sum_base_price", p[0].l_extendedprice), 
                                                               ("sum_disc_price", (p[0].l_extendedprice * (ConstantExpr(1.0) - p[0].l_discount))), 
                                                               ("sum_charge", ((p[0].l_extendedprice * (ConstantExpr(1.0) - p[0].l_discount)) * (ConstantExpr(1.0) + p[0].l_tax))), 
                                                               ("count_order", ConstantExpr(1))
                                                               ])
                                                  )]), 
                                    ConstantExpr(None)
                                    ), 
                   li, 
                   False
                   ), 
        LetExpr(results, 
                SumBuilder(lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]), 
                           lineitem_probed, 
                           True
                           ), 
                LetExpr(VarExpr("out"), results, ConstantExpr(True))))
```

# Q3
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(building, 
        ConstantExpr("BUILDING"), 
        LetExpr(customer_indexed, 
                JoinPartitionBuilder(cu, 
                                     "c_custkey", 
                                     lambda p: (p.c_mktsegment == building), 
                                     []
                ), 
                LetExpr(order_probed, 
                        JoinProbeBuilder(customer_indexed, 
                                         ord, 
                                         "o_custkey", 
                                         lambda p: (p.o_orderdate < ConstantExpr(19950315)), 
                                         lambda indexedDictValue, probeDictKey: 
                                         DicConsExpr([(probeDictKey.o_orderkey, 
                                                       RecConsExpr([("o_orderdate", probeDictKey.o_orderdate), 
                                                                    ("o_shippriority", probeDictKey.o_shippriority)]))]), 
                                         True), 
                        LetExpr(lineitem_probed, 
                                JoinProbeBuilder(order_probed, 
                                                 li, 
                                                 "l_orderkey", 
                                                 lambda p: (p.l_shipdate > ConstantExpr(19950315)), 
                                                 lambda indexedDictValue, probeDictKey: 
                                                 DicConsExpr([(RecConsExpr([("l_orderkey", probeDictKey.l_orderkey), 
                                                                            ("o_orderdate", indexedDictValue.o_orderdate), 
                                                                            ("o_shippriority", indexedDictValue.o_shippriority)]), 
                                                               RecConsExpr([("revenue", (probeDictKey.l_extendedprice * (ConstantExpr(1.0) - probeDictKey.l_discount)))]))])), 
                                LetExpr(results, 
                                        SumBuilder(lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]), lineitem_probed, True), 
                                        LetExpr(VarExpr("out"), results, ConstantExpr(True))
                                )
                        )
                )
        )
)
```

```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(building, 
        ConstantExpr("BUILDING"), 
        LetExpr(customer_indexed, 
                SumExpr(v1, 
                        db->cu_dataset, 
                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'c_mktsegment'), building), 
                               DicConsExpr([(
                                   RecAccessExpr(PairAccessExpr(v1, 0), 'c_custkey'), 
                                   RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(v1, 0), 'c_custkey'))]))]), 
                               EmptyDicConsExpr()
                        ), 
                        True
                ), 
                LetExpr(order_probed, 
                        LetExpr(v3, 
                                customer_indexed, 
                                SumExpr(v4, 
                                        db->ord_dataset, 
                                        IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderdate'), ConstantExpr(19950315)), 
                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v3, RecAccessExpr(PairAccessExpr(v4, 0), 'o_custkey')), ConstantExpr(None)), 
                                                      DicConsExpr([(
                                                              RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderkey'), 
                                                              RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderdate')), 
                                                                           ('o_shippriority', RecAccessExpr(PairAccessExpr(v4, 0), 'o_shippriority'))])
                                                      )]), 
                                                      EmptyDicConsExpr()
                                               ), 
                                               EmptyDicConsExpr()
                                        ), 
                                        True
                                )
                        ), 
                        LetExpr(lineitem_probed, 
                                LetExpr(v6, 
                                        order_probed, 
                                        SumExpr(v7, 
                                                db->li_dataset, 
                                                IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(v7, 0), 'l_shipdate'), ConstantExpr(19950315)), 
                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), ConstantExpr(None)), 
                                                              DicConsExpr([(
                                                                  RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 
                                                                               ('o_orderdate', RecAccessExpr(DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 'o_orderdate')), 
                                                                               ('o_shippriority', RecAccessExpr(DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 'o_shippriority'))]), 
                                                                  RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(v7, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v7, 0), 'l_discount'))))])
                                                              )]), 
                                                              EmptyDicConsExpr()
                                                       ), 
                                                       EmptyDicConsExpr()
                                                ), 
                                                False
                                        )
                                ), 
                                LetExpr(results, 
                                        SumExpr(v9, 
                                                lineitem_probed, 
                                                DicConsExpr([(ConcatExpr(PairAccessExpr(v9, 0), PairAccessExpr(v9, 1)), ConstantExpr(True))]), 
                                                True), 
                                        LetExpr(out, results, ConstantExpr(True))
                                )
                        )
                )
        )
)
```

# Q4
```python
LetExpr(VarExpr("li_indexed"), 
        SumExpr(VarExpr("v1"), 
                VarExpr("db->li_dataset"), 
                IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_commitdate'), RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_receiptdate')), 
                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_orderkey'), ConstantExpr(True))]), 
                       ConstantExpr(None)), 
                True), 
        LetExpr(VarExpr("ord_probed"), 
                LetExpr(VarExpr("v3"), 
                        VarExpr("li_indexed"), 
                        SumExpr(VarExpr("v4"), 
                                VarExpr("db->ord_dataset"), 
                                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'o_orderdate'), ConstantExpr(19930701)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'o_orderdate'), ConstantExpr(19931001))), 
                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("v3"), RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'o_orderkey')), ConstantExpr(None)), 
                                              DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'o_orderpriority'), 
                                                            ConstantExpr(1))]), 
                                              EmptyDicConsExpr()), 
                                       EmptyDicConsExpr()), 
                                False)), 
                LetExpr(VarExpr("results"), 
                        SumExpr(VarExpr("v6"), 
                                VarExpr("ord_probed"), 
                                DicConsExpr([(RecConsExpr([('o_orderpriority', PairAccessExpr(VarExpr("v6"), 0)), 
                                                           ('order_count', PairAccessExpr(VarExpr("v6"), 1))]), 
                                              ConstantExpr(True))]), 
                                True), 
                        LetExpr(VarExpr("out"), VarExpr("results"), ConstantExpr(True)))))
```

# Q6
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("results"), 
        SumExpr(VarExpr("v1"), 
                VarExpr("db->li_dataset"), 
                IfExpr(MulExpr(MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_shipdate'), ConstantExpr(19940101)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_shipdate'), ConstantExpr(19950101))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_discount'), ConstantExpr(0.05))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_discount'), ConstantExpr(0.07))), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_quantity'), ConstantExpr(24.0))), 
                       MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_extendedprice'), RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_discount')), ConstantExpr(0.0)), 
                False), 
        LetExpr(VarExpr("out"), VarExpr("results"), ConstantExpr(True)))
)
```

```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(results, 
        SumExpr(v1, 
                db->li_dataset, 
                IfExpr(MulExpr(MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(v1, 0), 'l_shipdate'), ConstantExpr(19940101)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v1, 0), 'l_shipdate'), ConstantExpr(19950101))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(v1, 0), 'l_discount'), ConstantExpr(0.05))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(v1, 0), 'l_discount'), ConstantExpr(0.07))), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v1, 0), 'l_quantity'), ConstantExpr(24.0))), 
                       MulExpr(RecAccessExpr(PairAccessExpr(v1, 0), 'l_extendedprice'), RecAccessExpr(PairAccessExpr(v1, 0), 'l_discount')), 
                       ConstantExpr(0.0)), 
        False
        ),
        LetExpr(out, results, ConstantExpr(True))
)
```

# Q10
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("r"), 
        ConstantExpr("R"), 
        LetExpr(VarExpr("na_indexed"), 
                SumExpr(VarExpr("v1"),
                        VarExpr("db->na_dataset"), 
                        IfExpr(ConstantExpr(True), 
                               DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'n_nationkey'), 
                                             RecConsExpr([('n_name', RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'n_name'))]))]), 
                               EmptyDicConsExpr()), 
                        True), 
                LetExpr(VarExpr("cu_indexed"), 
                        SumExpr(VarExpr("v3"), 
                                VarExpr("db->cu_dataset"), 
                                IfExpr(ConstantExpr(True), 
                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_custkey'), 
                                                     RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_custkey')), 
                                                                  ('c_name', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_name')), 
                                                                  ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_acctbal')), 
                                                                  ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_address')), 
                                                                  ('c_nationkey', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_nationkey')), 
                                                                  ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_phone')), 
                                                                  ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_comment'))]))]), 
                                       EmptyDicConsExpr()), 
                                True), 
                        LetExpr(VarExpr("ord_probed"), 
                                LetExpr(VarExpr("v5"), 
                                        VarExpr("cu_indexed"), 
                                        SumExpr(VarExpr("v6"), 
                                                VarExpr("db->ord_dataset"), 
                                                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_orderdate'), ConstantExpr(19940101))), 
                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), ConstantExpr(None)), 
                                                              DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_orderkey'), 
                                                                            RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_custkey')), 
                                                                                         ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_name')), 
                                                                                         ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_acctbal')), 
                                                                                         ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_address')), 
                                                                                         ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_phone')), 
                                                                                         ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_comment')), 
                                                                                         ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_indexed"), RecAccessExpr(DicLookupExpr(VarExpr("v5"), RecAccessExpr(PairAccessExpr(VarExpr("v6"), 0), 'o_custkey')), 'c_nationkey')), 'n_name'))]))]), 
                                                              EmptyDicConsExpr()), 
                                                       EmptyDicConsExpr()), 
                                                True)), 
                                LetExpr(VarExpr("li_probed"), 
                                        LetExpr(VarExpr("v8"), 
                                                VarExpr("ord_probed"), 
                                                SumExpr(VarExpr("v9"), 
                                                        VarExpr("db->li_dataset"), 
                                                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_returnflag'), VarExpr("r")), 
                                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                                      DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'c_custkey')), 
                                                                                                 ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'c_name')), 
                                                                                                 ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'c_acctbal')), 
                                                                                                 ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'n_name')), 
                                                                                                 ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'c_address')), 
                                                                                                 ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'c_phone')), 
                                                                                                 ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("v8"), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_orderkey')), 'c_comment'))]), 
                                                                                    MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(VarExpr("v9"), 0), 'l_discount'))))]), 
                                                                      EmptyDicConsExpr()), 
                                                               EmptyDicConsExpr()), 
                                                        False)), 
                                        LetExpr(VarExpr("results"), 
                                                SumExpr(VarExpr("v11"), 
                                                        VarExpr("li_probed"), 
                                                        DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'c_custkey')), 
                                                                                   ('c_name', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'c_name')), 
                                                                                   ('revenue', PairAccessExpr(VarExpr("v11"), 1)), 
                                                                                   ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'c_acctbal')), 
                                                                                   ('n_name', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'n_name')), 
                                                                                   ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'c_address')), 
                                                                                   ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'c_phone')), 
                                                                                   ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("v11"), 0), 'c_comment'))]), 
                                                                      ConstantExpr(True))]), 
                                                        True), 
                                                LetExpr(VarExpr("out"), 
                                                        VarExpr("results"), 
                                                        ConstantExpr(True))))))))
```

# Q19
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("brand12"), ConstantExpr("Brand#12"), 
        LetExpr(VarExpr("brand23"), ConstantExpr("Brand#23"), 
                LetExpr(VarExpr("brand34"), ConstantExpr("Brand#34"), 
                        LetExpr(VarExpr("smcase"), ConstantExpr("SM CASE"), 
                                LetExpr(VarExpr("smbox"), ConstantExpr("SM BOX"), 
                                        LetExpr(VarExpr("smpack"), ConstantExpr("SM PACK"), 
                                                LetExpr(VarExpr("smpkg"), ConstantExpr("SM PKG"), 
                                                        LetExpr(VarExpr("mdbag"), ConstantExpr("MED BAG"), 
                                                                LetExpr(VarExpr("mdbox"), ConstantExpr("MED BOX"), 
                                                                        LetExpr(VarExpr("mdpack"), ConstantExpr("MED PACK"), 
                                                                                LetExpr(VarExpr("mdpkg"), ConstantExpr("MED PKG"), 
                                                                                        LetExpr(VarExpr("lgcase"), ConstantExpr("LG CASE"), 
                                                                                                LetExpr(VarExpr("lgbox"), ConstantExpr("LG BOX"), 
                                                                                                        LetExpr(VarExpr("lgpack"), ConstantExpr("LG PACK"), 
                                                                                                                LetExpr(VarExpr("lgpkg"), ConstantExpr("LG PKG"), 
                                                                                                                        LetExpr(VarExpr("air"), ConstantExpr("AIR"), 
                                                                                                                                LetExpr(VarExpr("airreg"), ConstantExpr("AIR REG"), 
                                                                                                                                        LetExpr(VarExpr("deliverinperson"), ConstantExpr("DELIVER IN PERSON"), 
                                                                                                                                                LetExpr(VarExpr("pa_indexed"), 
                                                                                                                                                        SumExpr(VarExpr("v1"), 
                                                                                                                                                                VarExpr("db->pa_dataset"), 
                                                                                                                                                                IfExpr(AddExpr(AddExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_brand'), VarExpr("brand12")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("smcase")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("smbox"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("smpack"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("smpkg")))), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size'), ConstantExpr(1)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size'), ConstantExpr(5)))), MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_brand'), VarExpr("brand23")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("mdbag")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("mdbox"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("mdpack"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("mdpkg")))), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size'), ConstantExpr(1)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size'), ConstantExpr(10))))), MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_brand'), VarExpr("brand34")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("lgcase")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("lgbox"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("lgpack"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'), VarExpr("lgpkg")))), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size'), ConstantExpr(1)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size'), ConstantExpr(15))))), 
                                                                                                                                                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_partkey'), 
                                                                                                                                                                                     RecConsExpr([('p_brand', RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_brand')), 
                                                                                                                                                                                                  ('p_size', RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_size')), 
                                                                                                                                                                                                  ('p_container', RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'p_container'))]))]), 
                                                                                                                                                                       EmptyDicConsExpr()), 
                                                                                                                                                                True), 
                                                                                                                                                        LetExpr(VarExpr("li_probed"), 
                                                                                                                                                                LetExpr(VarExpr("v3"), 
                                                                                                                                                                        VarExpr("pa_indexed"), 
                                                                                                                                                                        SumExpr(VarExpr("v4"), 
                                                                                                                                                                                VarExpr("db->li_dataset"), 
                                                                                                                                                                                IfExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_shipinstruct'), VarExpr("deliverinperson")), AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_shipmode'), VarExpr("air")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_shipmode'), VarExpr("airreg")))), 
                                                                                                                                                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("v3"), RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_partkey')), ConstantExpr(None)), 
                                                                                                                                                                                              IfExpr(AddExpr(AddExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(DicLookupExpr(VarExpr("v3"), RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_partkey')), 'p_brand'), VarExpr("brand12")), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_quantity'), ConstantExpr(1)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_quantity'), ConstantExpr(11)))), MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(DicLookupExpr(VarExpr("v3"), RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_partkey')), 'p_brand'), VarExpr("brand23")), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_quantity'), ConstantExpr(10)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_quantity'), ConstantExpr(20))))), MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(DicLookupExpr(VarExpr("v3"), RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_partkey')), 'p_brand'), VarExpr("brand34")), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_quantity'), ConstantExpr(20)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_quantity'), ConstantExpr(30))))), 
                                                                                                                                                                                                     MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(VarExpr("v4"), 0), 'l_discount'))), ConstantExpr(None)), 
                                                                                                                                                                                              EmptyDicConsExpr()), 
                                                                                                                                                                                       EmptyDicConsExpr()), 
                                                                                                                                                                                False)), 
                                                                                                                                                                LetExpr(VarExpr("results"), 
                                                                                                                                                                        DicConsExpr([(RecConsExpr([('revenue', VarExpr("li_probed"))]), ConstantExpr(True))]), 
                                                                                                                                                                        LetExpr(VarExpr("out"), 
                                                                                                                                                                                VarExpr("results"), 
                                                                                                                                                                                ConstantExpr(True)))))))))))))))))))))))

```