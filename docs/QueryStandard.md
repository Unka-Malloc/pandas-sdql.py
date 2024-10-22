# Q1
```python
LetExpr(lineitem_probed, 
        SumExpr(v1, 
                li, 
                IfExpr(CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(v1, 0), 'l_shipdate'), ConstantExpr(19980902)), 
                       DicConsExpr([(RecConsExpr([('l_returnflag', RecAccessExpr(PairAccessExpr(v1, 0), 'l_returnflag')), 
                                                  ('l_linestatus', RecAccessExpr(PairAccessExpr(v1, 0), 'l_linestatus'))]), 
                                     RecConsExpr([('sum_qty', RecAccessExpr(PairAccessExpr(v1, 0), 'l_quantity')), 
                                                  ('sum_base_price', RecAccessExpr(PairAccessExpr(v1, 0), 'l_extendedprice')),
                                                  ('sum_disc_price', MulExpr(RecAccessExpr(PairAccessExpr(v1, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v1, 0), 'l_discount')))), 
                                                  ('sum_charge', MulExpr(MulExpr(RecAccessExpr(PairAccessExpr(v1, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v1, 0), 'l_discount'))), AddExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v1, 0), 'l_tax')))), 
                                                  ('count_order', ConstantExpr(1))]))]), 
                       ConstantExpr(None)), 
                False), 
        LetExpr(results, 
                SumExpr(v3, 
                        lineitem_probed, 
                        DicConsExpr([(ConcatExpr(PairAccessExpr(v3, 0), PairAccessExpr(v3, 1)), ConstantExpr(True))]), 
                        True), 
                LetExpr(out, results, ConstantExpr(True))))
```

# Q3
```python
LetExpr(building, 
        ConstantExpr("BUILDING"), 
        LetExpr(customer_indexed, 
                SumExpr(v1, 
                        cu, 
                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'c_mktsegment'), building), 
                               DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'c_custkey'), 
                                             RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(v1, 0), 'c_custkey'))]))]), 
                               EmptyDicConsExpr()), 
                        True), 
                LetExpr(order_probed, 
                        LetExpr(v3, 
                                customer_indexed, 
                                SumExpr(v4, 
                                        ord, 
                                        IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderdate'), ConstantExpr(19950315)), 
                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v3, RecAccessExpr(PairAccessExpr(v4, 0), 'o_custkey')), ConstantExpr(None)), 
                                                      DicConsExpr([(RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderkey'), 
                                                                    RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(v4, 0), 'o_orderdate')), 
                                                                                 ('o_shippriority', RecAccessExpr(PairAccessExpr(v4, 0), 'o_shippriority'))]))]), 
                                                      EmptyDicConsExpr()), 
                                               EmptyDicConsExpr()), 
                                        True)), 
                        LetExpr(lineitem_probed, 
                                LetExpr(v6, 
                                        order_probed, 
                                        SumExpr(v7, 
                                                li, 
                                                IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(v7, 0), 'l_shipdate'), ConstantExpr(19950315)), 
                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), ConstantExpr(None)), 
                                                              DicConsExpr([(RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 
                                                                                         ('o_orderdate', RecAccessExpr(DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 'o_orderdate')), 
                                                                                         ('o_shippriority', RecAccessExpr(DicLookupExpr(v6, RecAccessExpr(PairAccessExpr(v7, 0), 'l_orderkey')), 'o_shippriority'))]), 
                                                                            RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(v7, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v7, 0), 'l_discount'))))]))]), 
                                                              EmptyDicConsExpr()), 
                                                       EmptyDicConsExpr()), 
                                                False)), 
                                LetExpr(results, 
                                        SumExpr(v9, 
                                                lineitem_probed, 
                                                DicConsExpr([(ConcatExpr(PairAccessExpr(v9, 0), PairAccessExpr(v9, 1)), ConstantExpr(True))]), 
                                                True), 
                                        LetExpr(out, results, ConstantExpr(True)))))))
```

# Q4
```python
LetExpr(VarExpr("li_indexed"), 
        SumExpr(VarExpr("v1"), 
                VarExpr("db->li_dataset"), 
                IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_commitdate'), RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_receiptdate')), 
                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_orderkey'), 
                                     ConstantExpr(True))]), 
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
from pysdql.core.prototype.basic.sdql_ir import *

LetExpr(VarExpr("results"),
        SumExpr(VarExpr("v1"),
                VarExpr("db->li_dataset"),
                IfExpr(MulExpr(MulExpr(MulExpr(MulExpr(
                    CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_shipdate'),
                                ConstantExpr(19940101)),
                    CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_shipdate'),
                                ConstantExpr(19950101))), CompareExpr(CompareSymbol.GTE,
                                                                      RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0),
                                                                                    'l_discount'), ConstantExpr(0.05))),
                    CompareExpr(CompareSymbol.LTE,
                                RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_discount'),
                                ConstantExpr(0.07))), CompareExpr(CompareSymbol.LT, RecAccessExpr(
                    PairAccessExpr(VarExpr("v1"), 0), 'l_quantity'), ConstantExpr(24.0))),
                    MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_extendedprice'),
                            RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'l_discount')),
                    ConstantExpr(0.0)),
                False),
        LetExpr(VarExpr("out"), VarExpr("results"), ConstantExpr(True)))
```

# Q10

```python
from pysdql.core.prototype.basic.sdql_ir import *

LetExpr(VarExpr("r"),
        ConstantExpr("R"),
        LetExpr(VarExpr("na_indexed"),
                SumExpr(VarExpr("v1"),
                        VarExpr("db->na_dataset"),
                        IfExpr(ConstantExpr(True),
                               DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0), 'n_nationkey'),
                                             RecConsExpr([('n_name', RecAccessExpr(PairAccessExpr(VarExpr("v1"), 0),
                                                                                   'n_name'))]))]),
                               EmptyDicConsExpr()),
                        True),
                LetExpr(VarExpr("cu_indexed"),
                        SumExpr(VarExpr("v3"),
                                VarExpr("db->cu_dataset"),
                                IfExpr(ConstantExpr(True),
                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0), 'c_custkey'),
                                                     RecConsExpr([('c_custkey',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_custkey')),
                                                                  ('c_name',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_name')),
                                                                  ('c_acctbal',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_acctbal')),
                                                                  ('c_address',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_address')),
                                                                  ('c_nationkey',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_nationkey')),
                                                                  ('c_phone',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_phone')),
                                                                  ('c_comment',
                                                                   RecAccessExpr(PairAccessExpr(VarExpr("v3"), 0),
                                                                                 'c_comment'))]))]),
                                       EmptyDicConsExpr()),
                                True),
                        LetExpr(VarExpr("ord_probed"),
                                LetExpr(VarExpr("v5"),
                                        VarExpr("cu_indexed"),
                                        SumExpr(VarExpr("v6"),
                                                VarExpr("db->ord_dataset"),
                                                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(
                                                    PairAccessExpr(VarExpr("v6"), 0), 'o_orderdate'),
                                                                           ConstantExpr(19931001)),
                                                               CompareExpr(CompareSymbol.LT, RecAccessExpr(
                                                                   PairAccessExpr(VarExpr("v6"), 0), 'o_orderdate'),
                                                                           ConstantExpr(19940101))),
                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("v5"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v6"),
                                                                                                                  0),
                                                                                                              'o_custkey')),
                                                                          ConstantExpr(None)),
                                                              DicConsExpr([(RecAccessExpr(
                                                                  PairAccessExpr(VarExpr("v6"), 0), 'o_orderkey'),
                                                                            RecConsExpr([('c_custkey', RecAccessExpr(
                                                                                DicLookupExpr(VarExpr("v5"),
                                                                                              RecAccessExpr(
                                                                                                  PairAccessExpr(
                                                                                                      VarExpr("v6"), 0),
                                                                                                  'o_custkey')),
                                                                                'c_custkey')),
                                                                                         ('c_name', RecAccessExpr(
                                                                                             DicLookupExpr(
                                                                                                 VarExpr("v5"),
                                                                                                 RecAccessExpr(
                                                                                                     PairAccessExpr(
                                                                                                         VarExpr("v6"),
                                                                                                         0),
                                                                                                     'o_custkey')),
                                                                                             'c_name')),
                                                                                         ('c_acctbal', RecAccessExpr(
                                                                                             DicLookupExpr(
                                                                                                 VarExpr("v5"),
                                                                                                 RecAccessExpr(
                                                                                                     PairAccessExpr(
                                                                                                         VarExpr("v6"),
                                                                                                         0),
                                                                                                     'o_custkey')),
                                                                                             'c_acctbal')),
                                                                                         ('c_address', RecAccessExpr(
                                                                                             DicLookupExpr(
                                                                                                 VarExpr("v5"),
                                                                                                 RecAccessExpr(
                                                                                                     PairAccessExpr(
                                                                                                         VarExpr("v6"),
                                                                                                         0),
                                                                                                     'o_custkey')),
                                                                                             'c_address')),
                                                                                         ('c_phone', RecAccessExpr(
                                                                                             DicLookupExpr(
                                                                                                 VarExpr("v5"),
                                                                                                 RecAccessExpr(
                                                                                                     PairAccessExpr(
                                                                                                         VarExpr("v6"),
                                                                                                         0),
                                                                                                     'o_custkey')),
                                                                                             'c_phone')),
                                                                                         ('c_comment', RecAccessExpr(
                                                                                             DicLookupExpr(
                                                                                                 VarExpr("v5"),
                                                                                                 RecAccessExpr(
                                                                                                     PairAccessExpr(
                                                                                                         VarExpr("v6"),
                                                                                                         0),
                                                                                                     'o_custkey')),
                                                                                             'c_comment')),
                                                                                         ('n_name', RecAccessExpr(
                                                                                             DicLookupExpr(
                                                                                                 VarExpr("na_indexed"),
                                                                                                 RecAccessExpr(
                                                                                                     DicLookupExpr(
                                                                                                         VarExpr("v5"),
                                                                                                         RecAccessExpr(
                                                                                                             PairAccessExpr(
                                                                                                                 VarExpr(
                                                                                                                     "v6"),
                                                                                                                 0),
                                                                                                             'o_custkey')),
                                                                                                     'c_nationkey')),
                                                                                             'n_name'))]))]),
                                                              EmptyDicConsExpr()),
                                                       EmptyDicConsExpr()),
                                                True)),
                                LetExpr(VarExpr("li_probed"),
                                        LetExpr(VarExpr("v8"),
                                                VarExpr("ord_probed"),
                                                SumExpr(VarExpr("v9"),
                                                        VarExpr("db->li_dataset"),
                                                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(
                                                            PairAccessExpr(VarExpr("v9"), 0), 'l_returnflag'),
                                                                           VarExpr("r")),
                                                               IfExpr(CompareExpr(CompareSymbol.NE,
                                                                                  DicLookupExpr(VarExpr("v8"),
                                                                                                RecAccessExpr(
                                                                                                    PairAccessExpr(
                                                                                                        VarExpr("v9"),
                                                                                                        0),
                                                                                                    'l_orderkey')),
                                                                                  ConstantExpr(None)),
                                                                      DicConsExpr([(RecConsExpr([('c_custkey',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'c_custkey')),
                                                                                                 ('c_name',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'c_name')),
                                                                                                 ('c_acctbal',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'c_acctbal')),
                                                                                                 ('n_name',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'n_name')),
                                                                                                 ('c_address',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'c_address')),
                                                                                                 ('c_phone',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'c_phone')),
                                                                                                 ('c_comment',
                                                                                                  RecAccessExpr(
                                                                                                      DicLookupExpr(
                                                                                                          VarExpr("v8"),
                                                                                                          RecAccessExpr(
                                                                                                              PairAccessExpr(
                                                                                                                  VarExpr(
                                                                                                                      "v9"),
                                                                                                                  0),
                                                                                                              'l_orderkey')),
                                                                                                      'c_comment'))]),
                                                                                    MulExpr(RecAccessExpr(
                                                                                        PairAccessExpr(VarExpr("v9"),
                                                                                                       0),
                                                                                        'l_extendedprice'),
                                                                                        SubExpr(ConstantExpr(1.0),
                                                                                                RecAccessExpr(
                                                                                                    PairAccessExpr(
                                                                                                        VarExpr(
                                                                                                            "v9"),
                                                                                                        0),
                                                                                                    'l_discount'))))]),
                                                                      EmptyDicConsExpr()),
                                                               EmptyDicConsExpr()),
                                                        False)),
                                        LetExpr(VarExpr("results"),
                                                SumExpr(VarExpr("v11"),
                                                        VarExpr("li_probed"),
                                                        DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(
                                                            PairAccessExpr(VarExpr("v11"), 0), 'c_custkey')),
                                                                                   ('c_name', RecAccessExpr(
                                                                                       PairAccessExpr(VarExpr("v11"),
                                                                                                      0), 'c_name')),
                                                                                   ('revenue',
                                                                                    PairAccessExpr(VarExpr("v11"), 1)),
                                                                                   ('c_acctbal', RecAccessExpr(
                                                                                       PairAccessExpr(VarExpr("v11"),
                                                                                                      0), 'c_acctbal')),
                                                                                   ('n_name', RecAccessExpr(
                                                                                       PairAccessExpr(VarExpr("v11"),
                                                                                                      0), 'n_name')),
                                                                                   ('c_address', RecAccessExpr(
                                                                                       PairAccessExpr(VarExpr("v11"),
                                                                                                      0), 'c_address')),
                                                                                   ('c_phone', RecAccessExpr(
                                                                                       PairAccessExpr(VarExpr("v11"),
                                                                                                      0), 'c_phone')),
                                                                                   ('c_comment', RecAccessExpr(
                                                                                       PairAccessExpr(VarExpr("v11"),
                                                                                                      0),
                                                                                       'c_comment'))]),
                                                                      ConstantExpr(True))]),
                                                        True),
                                                LetExpr(VarExpr("out"),
                                                        VarExpr("results"),
                                                        ConstantExpr(True))))))))
```

# Q14
```python
LetExpr(promo, 
        ConstantExpr("PROMO"), 
        LetExpr(pa_indexed, 
                SumExpr(v1, 
                        pa, 
                        IfExpr(ExtFuncExpr(ExtFuncSymbol.StartsWith, RecAccessExpr(PairAccessExpr(v1, 0), 'p_type'), promo, ConstantExpr("Nothing!")), 
                               DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'p_partkey'), 
                                             RecConsExpr([('p_partkey', RecAccessExpr(PairAccessExpr(v1, 0), 'p_partkey'))]))]), 
                               EmptyDicConsExpr()), 
                        True), 
                LetExpr(li_probed, 
                        SumExpr(v3, 
                                li, 
                                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(v3, 0), 'l_shipdate'), ConstantExpr(19950901)), 
                                               CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v3, 0), 'l_shipdate'), ConstantExpr(19951001))), 
                                       RecConsExpr([('A', 
                                                     IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(pa_indexed, RecAccessExpr(PairAccessExpr(v3, 0), 'l_partkey')), ConstantExpr(None)), 
                                                            MulExpr(RecAccessExpr(PairAccessExpr(v3, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v3, 0), 'l_discount'))), ConstantExpr(0.0))), 
                                                    ('B', 
                                                     MulExpr(RecAccessExpr(PairAccessExpr(v3, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v3, 0), 'l_discount'))))]), 
                                       ConstantExpr(None)),
                                False), 
                        LetExpr(results, 
                                DivExpr(MulExpr(ConstantExpr(100.0), RecAccessExpr(li_probed, 'A')), RecAccessExpr(li_probed, 'B')), 
                                LetExpr(out, results, ConstantExpr(True))))))
```

# Q15
```python
LetExpr(li_aggr, 
        SumExpr(v1, 
                li,
                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(v1, 0), 'l_shipdate'), ConstantExpr(19960101)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(v1, 0), 'l_shipdate'), ConstantExpr(19960401))), 
                       DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'l_suppkey'), 
                                     MulExpr(RecAccessExpr(PairAccessExpr(v1, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1.0), RecAccessExpr(PairAccessExpr(v1, 0), 'l_discount'))))]), 
                       ConstantExpr(None)), 
                False), 
        LetExpr(max_revenue, 
                ConstantExpr(1772627.2087), 
                LetExpr(su_indexed, 
                        SumExpr(v3, 
                                su, 
                                IfExpr(ConstantExpr(True), 
                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(v3, 0), 's_suppkey'), 
                                                     RecConsExpr([('s_name', RecAccessExpr(PairAccessExpr(v3, 0), 's_name')), 
                                                                  ('s_address', RecAccessExpr(PairAccessExpr(v3, 0), 's_address')), 
                                                                  ('s_phone', RecAccessExpr(PairAccessExpr(v3, 0), 's_phone'))]))]), 
                                       EmptyDicConsExpr()), 
                                True), 
                        LetExpr(results, 
                                SumExpr(v5, 
                                        li_aggr,
                                        IfExpr(CompareExpr(CompareSymbol.EQ, PairAccessExpr(v5, 1), max_revenue), 
                                               DicConsExpr([(RecConsExpr([('s_suppkey', PairAccessExpr(v5, 0)), 
                                                                          ('s_name', RecAccessExpr(DicLookupExpr(su_indexed, PairAccessExpr(v5, 0)), 's_name')), 
                                                                          ('s_address', RecAccessExpr(DicLookupExpr(su_indexed, PairAccessExpr(v5, 0)), 's_address')),
                                                                          ('s_phone', RecAccessExpr(DicLookupExpr(su_indexed, PairAccessExpr(v5, 0)), 's_phone')), 
                                                                          ('total_revenue', PairAccessExpr(v5, 1))]), 
                                                             ConstantExpr(True))]), 
                                               ConstantExpr(None)), 
                                        True), 
                                LetExpr(out, results, ConstantExpr(True))))))
```

# Q16
```python
LetExpr(brand45, ConstantExpr("Brand#45"), 
        LetExpr(medpol, ConstantExpr("MEDIUM POLISHED"), 
                LetExpr(Customer, ConstantExpr("Customer"), 
                        LetExpr(complaints, ConstantExpr("Complaints"), 
                                LetExpr(part_indexed, 
                                        SumExpr(v1, 
                                                pa, 
                                                IfExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.NE, RecAccessExpr(PairAccessExpr(v1, 0), 'p_brand'), brand45), 
                                                                       CompareExpr(CompareSymbol.EQ, 
                                                                                   ExtFuncExpr(ExtFuncSymbol.StartsWith, RecAccessExpr(PairAccessExpr(v1, 0), 'p_type'), medpol, ConstantExpr("Nothing!")), 
                                                                                   ConstantExpr(False))), 
                                                               AddExpr(AddExpr(AddExpr(AddExpr(AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(49)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(14))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(23))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(45))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(19))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(3))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(36))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'), ConstantExpr(9)))), 
                                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'p_partkey'), 
                                                                     RecConsExpr([('p_brand', RecAccessExpr(PairAccessExpr(v1, 0), 'p_brand')), 
                                                                                  ('p_type', RecAccessExpr(PairAccessExpr(v1, 0), 'p_type')), 
                                                                                  ('p_size', RecAccessExpr(PairAccessExpr(v1, 0), 'p_size'))]))]), 
                                                       EmptyDicConsExpr()), 
                                                True), 
                                        LetExpr(su_indexed, 
                                                SumExpr(v3,
                                                        su,
                                                        IfExpr(MulExpr(CompareExpr(CompareSymbol.NE, ExtFuncExpr(ExtFuncSymbol.FirstIndex, RecAccessExpr(PairAccessExpr(v3, 0), 's_comment'), Customer, ConstantExpr("Nothing!")), MulExpr(ConstantExpr(-1), ConstantExpr(1))), 
                                                                       CompareExpr(CompareSymbol.GT, ExtFuncExpr(ExtFuncSymbol.FirstIndex, RecAccessExpr(PairAccessExpr(v3, 0), 's_comment'), complaints, ConstantExpr("Nothing!")), 
                                                                                   AddExpr(ExtFuncExpr(ExtFuncSymbol.FirstIndex, RecAccessExpr(PairAccessExpr(v3, 0), 's_comment'), Customer, ConstantExpr("Nothing!")), ConstantExpr(7)))), 
                                                               DicConsExpr([(RecAccessExpr(PairAccessExpr(v3, 0), 's_suppkey'), 
                                                                             RecConsExpr([('s_suppkey', RecAccessExpr(PairAccessExpr(v3, 0), 's_suppkey'))]))]), 
                                                               EmptyDicConsExpr()), 
                                                        True), 
                                                LetExpr(partsupp_probe, 
                                                        LetExpr(v5, 
                                                                part_indexed, 
                                                                SumExpr(v6, 
                                                                        ps, 
                                                                        IfExpr(ConstantExpr(True), 
                                                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v5, RecAccessExpr(PairAccessExpr(v6, 0), 'ps_partkey')), ConstantExpr(None)), 
                                                                                      IfExpr(CompareExpr(CompareSymbol.EQ, DicLookupExpr(su_indexed, RecAccessExpr(PairAccessExpr(v6, 0), 'ps_suppkey')), ConstantExpr(None)), 
                                                                                             DicConsExpr([(RecConsExpr([('p_brand', RecAccessExpr(DicLookupExpr(v5, RecAccessExpr(PairAccessExpr(v6, 0), 'ps_partkey')), 'p_brand')),
                                                                                                                        ('p_type', RecAccessExpr(DicLookupExpr(v5, RecAccessExpr(PairAccessExpr(v6, 0), 'ps_partkey')), 'p_type')),
                                                                                                                        ('p_size', RecAccessExpr(DicLookupExpr(v5, RecAccessExpr(PairAccessExpr(v6, 0), 'ps_partkey')), 'p_size'))]),
                                                                                                           DicConsExpr([(RecAccessExpr(PairAccessExpr(v6, 0), 'ps_suppkey'), 
                                                                                                                         ConstantExpr(True))]))]), 
                                                                                             ConstantExpr(None)), 
                                                                                      EmptyDicConsExpr()),
                                                                               EmptyDicConsExpr()),
                                                                        False)), 
                                                        LetExpr(results,
                                                                SumExpr(v8, 
                                                                        partsupp_probe, 
                                                                        DicConsExpr([(ConcatExpr(PairAccessExpr(v8, 0), RecConsExpr([('supplier_cnt', ExtFuncExpr(ExtFuncSymbol.DictSize, PairAccessExpr(v8, 1), ConstantExpr("Nothing!"), ConstantExpr("Nothing!")))])), 
                                                                                      ConstantExpr(True))]), 
                                                                        False), 
                                                                LetExpr(out, results, ConstantExpr(True))))))))))
```

# Q18
```python
LetExpr(li_aggregated, 
        SumExpr(v1, 
                li,
                DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'l_orderkey'), 
                              RecAccessExpr(PairAccessExpr(v1, 0), 'l_quantity'))]), False), 
        LetExpr(li_filtered, 
                SumExpr(v3, 
                        li_aggregated, 
                        IfExpr(CompareExpr(CompareSymbol.GT, PairAccessExpr(v3, 1), ConstantExpr(300)), 
                               DicConsExpr([(PairAccessExpr(v3, 0), ConstantExpr(True))]), 
                               ConstantExpr(None)), 
                        True), 
                LetExpr(cu_indexed, 
                        SumExpr(v5, 
                                cu, 
                                IfExpr(ConstantExpr(True), 
                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(v5, 0), 'c_custkey'), 
                                                    RecConsExpr([('c_name', RecAccessExpr(PairAccessExpr(v5, 0), 'c_name'))]))]), 
                                       EmptyDicConsExpr()), 
                                True), LetExpr(order_probed, 
                                              LetExpr(v7, 
                                                      cu_indexed, 
                                                      SumExpr(v8, 
                                                              ord, 
                                                              IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(li_filtered, RecAccessExpr(PairAccessExpr(v8, 0), 'o_orderkey')), ConstantExpr(None)), 
                                                                     IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v7, RecAccessExpr(PairAccessExpr(v8, 0), 'o_custkey')), ConstantExpr(None)), 
                                                                            DicConsExpr([(RecAccessExpr(PairAccessExpr(v8, 0), 'o_orderkey'), 
                                                                                          RecConsExpr([('c_name', RecAccessExpr(DicLookupExpr(v7, RecAccessExpr(PairAccessExpr(v8, 0), 'o_custkey')), 'c_name')), 
                                                                                                       ('o_custkey', RecAccessExpr(PairAccessExpr(v8, 0), 'o_custkey')),
                                                                                                       ('o_orderkey', RecAccessExpr(PairAccessExpr(v8, 0), 'o_orderkey')), 
                                                                                                       ('o_orderdate', RecAccessExpr(PairAccessExpr(v8, 0), 'o_orderdate')), 
                                                                                                       ('o_totalprice', RecAccessExpr(PairAccessExpr(v8, 0), 'o_totalprice'))]))]), 
                                                                            EmptyDicConsExpr()), 
                                                                     EmptyDicConsExpr()), 
                                                              True)), 
                                              LetExpr(li_probed, 
                                                      LetExpr(v10, 
                                                              order_probed, 
                                                              SumExpr(v11, 
                                                                      li, 
                                                                      IfExpr(ConstantExpr(True), 
                                                                             IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(v10, RecAccessExpr(PairAccessExpr(v11, 0), 'l_orderkey')), ConstantExpr(None)),
                                                                                    DicConsExpr([(RecConsExpr([('c_name', RecAccessExpr(DicLookupExpr(v10, RecAccessExpr(PairAccessExpr(v11, 0), 'l_orderkey')), 'c_name')),
                                                                                                               ('o_custkey', RecAccessExpr(DicLookupExpr(v10, RecAccessExpr(PairAccessExpr(v11, 0), 'l_orderkey')), 'o_custkey')),
                                                                                                               ('o_orderkey', RecAccessExpr(DicLookupExpr(v10, RecAccessExpr(PairAccessExpr(v11, 0), 'l_orderkey')), 'o_orderkey')), 
                                                                                                               ('o_orderdate', RecAccessExpr(DicLookupExpr(v10, RecAccessExpr(PairAccessExpr(v11, 0), 'l_orderkey')), 'o_orderdate')),
                                                                                                               ('o_totalprice', RecAccessExpr(DicLookupExpr(v10, RecAccessExpr(PairAccessExpr(v11, 0), 'l_orderkey')), 'o_totalprice'))]),
                                                                                                  RecConsExpr([('quantitysum', RecAccessExpr(PairAccessExpr(v11, 0), 'l_quantity'))]))]), 
                                                                                    EmptyDicConsExpr()), 
                                                                             EmptyDicConsExpr()), 
                                                                      False)), 
                                                      LetExpr(results, 
                                                              SumExpr(v13, 
                                                                      li_probed, 
                                                                      DicConsExpr([(ConcatExpr(PairAccessExpr(v13, 0), PairAccessExpr(v13, 1)), ConstantExpr(True))]), True), 
                                                              LetExpr(out, results, ConstantExpr(True))))))))
```

# Q19

```python
from pysdql.core.prototype.basic.sdql_ir import *

LetExpr(VarExpr("brand12"), ConstantExpr("Brand#12"),
        LetExpr(VarExpr("brand23"), ConstantExpr("Brand#23"),
                LetExpr(VarExpr("brand34"), ConstantExpr("Brand#34"),
                        LetExpr(VarExpr("smcase"), ConstantExpr("SM CASE"),
                                LetExpr(VarExpr("smbox"), ConstantExpr("SM BOX"),
                                        LetExpr(VarExpr("smpack"), ConstantExpr("SM PACK"),
                                                LetExpr(VarExpr("smpkg"), ConstantExpr("SM PKG"),
                                                        LetExpr(VarExpr("mdbag"), ConstantExpr("MED BAG"),
                                                                LetExpr(VarExpr("mdbox"), ConstantExpr("MED BOX"),
                                                                        LetExpr(VarExpr("mdpack"),
                                                                                ConstantExpr("MED PACK"),
                                                                                LetExpr(VarExpr("mdpkg"),
                                                                                        ConstantExpr("MED PKG"),
                                                                                        LetExpr(VarExpr("lgcase"),
                                                                                                ConstantExpr("LG CASE"),
                                                                                                LetExpr(
                                                                                                    VarExpr("lgbox"),
                                                                                                    ConstantExpr(
                                                                                                        "LG BOX"),
                                                                                                    LetExpr(VarExpr(
                                                                                                        "lgpack"),
                                                                                                        ConstantExpr(
                                                                                                            "LG PACK"),
                                                                                                        LetExpr(
                                                                                                            VarExpr(
                                                                                                                "lgpkg"),
                                                                                                            ConstantExpr(
                                                                                                                "LG PKG"),
                                                                                                            LetExpr(
                                                                                                                VarExpr(
                                                                                                                    "air"),
                                                                                                                ConstantExpr(
                                                                                                                    "AIR"),
                                                                                                                LetExpr(
                                                                                                                    VarExpr(
                                                                                                                        "airreg"),
                                                                                                                    ConstantExpr(
                                                                                                                        "AIR REG"),
                                                                                                                    LetExpr(
                                                                                                                        VarExpr(
                                                                                                                            "deliverinperson"),
                                                                                                                        ConstantExpr(
                                                                                                                            "DELIVER IN PERSON"),
                                                                                                                        LetExpr(
                                                                                                                            VarExpr(
                                                                                                                                "pa_indexed"),
                                                                                                                            SumExpr(
                                                                                                                                VarExpr(
                                                                                                                                    "v1"),
                                                                                                                                VarExpr(
                                                                                                                                    "db->pa_dataset"),
                                                                                                                                IfExpr(
                                                                                                                                    AddExpr(
                                                                                                                                        AddExpr(
                                                                                                                                            MulExpr(
                                                                                                                                                MulExpr(
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.EQ,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_brand'),
                                                                                                                                                        VarExpr(
                                                                                                                                                            "brand12")),
                                                                                                                                                    AddExpr(
                                                                                                                                                        AddExpr(
                                                                                                                                                            AddExpr(
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        PairAccessExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v1"),
                                                                                                                                                                            0),
                                                                                                                                                                        'p_container'),
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "smcase")),
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        PairAccessExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v1"),
                                                                                                                                                                            0),
                                                                                                                                                                        'p_container'),
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "smbox"))),
                                                                                                                                                            CompareExpr(
                                                                                                                                                                CompareSymbol.EQ,
                                                                                                                                                                RecAccessExpr(
                                                                                                                                                                    PairAccessExpr(
                                                                                                                                                                        VarExpr(
                                                                                                                                                                            "v1"),
                                                                                                                                                                        0),
                                                                                                                                                                    'p_container'),
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "smpack"))),
                                                                                                                                                        CompareExpr(
                                                                                                                                                            CompareSymbol.EQ,
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v1"),
                                                                                                                                                                    0),
                                                                                                                                                                'p_container'),
                                                                                                                                                            VarExpr(
                                                                                                                                                                "smpkg")))),
                                                                                                                                                MulExpr(
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.GTE,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_size'),
                                                                                                                                                        ConstantExpr(
                                                                                                                                                            1)),
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.LTE,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_size'),
                                                                                                                                                        ConstantExpr(
                                                                                                                                                            5)))),
                                                                                                                                            MulExpr(
                                                                                                                                                MulExpr(
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.EQ,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_brand'),
                                                                                                                                                        VarExpr(
                                                                                                                                                            "brand23")),
                                                                                                                                                    AddExpr(
                                                                                                                                                        AddExpr(
                                                                                                                                                            AddExpr(
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        PairAccessExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v1"),
                                                                                                                                                                            0),
                                                                                                                                                                        'p_container'),
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "mdbag")),
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        PairAccessExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v1"),
                                                                                                                                                                            0),
                                                                                                                                                                        'p_container'),
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "mdbox"))),
                                                                                                                                                            CompareExpr(
                                                                                                                                                                CompareSymbol.EQ,
                                                                                                                                                                RecAccessExpr(
                                                                                                                                                                    PairAccessExpr(
                                                                                                                                                                        VarExpr(
                                                                                                                                                                            "v1"),
                                                                                                                                                                        0),
                                                                                                                                                                    'p_container'),
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "mdpack"))),
                                                                                                                                                        CompareExpr(
                                                                                                                                                            CompareSymbol.EQ,
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v1"),
                                                                                                                                                                    0),
                                                                                                                                                                'p_container'),
                                                                                                                                                            VarExpr(
                                                                                                                                                                "mdpkg")))),
                                                                                                                                                MulExpr(
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.GTE,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_size'),
                                                                                                                                                        ConstantExpr(
                                                                                                                                                            1)),
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.LTE,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_size'),
                                                                                                                                                        ConstantExpr(
                                                                                                                                                            10))))),
                                                                                                                                        MulExpr(
                                                                                                                                            MulExpr(
                                                                                                                                                CompareExpr(
                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                    RecAccessExpr(
                                                                                                                                                        PairAccessExpr(
                                                                                                                                                            VarExpr(
                                                                                                                                                                "v1"),
                                                                                                                                                            0),
                                                                                                                                                        'p_brand'),
                                                                                                                                                    VarExpr(
                                                                                                                                                        "brand34")),
                                                                                                                                                AddExpr(
                                                                                                                                                    AddExpr(
                                                                                                                                                        AddExpr(
                                                                                                                                                            CompareExpr(
                                                                                                                                                                CompareSymbol.EQ,
                                                                                                                                                                RecAccessExpr(
                                                                                                                                                                    PairAccessExpr(
                                                                                                                                                                        VarExpr(
                                                                                                                                                                            "v1"),
                                                                                                                                                                        0),
                                                                                                                                                                    'p_container'),
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "lgcase")),
                                                                                                                                                            CompareExpr(
                                                                                                                                                                CompareSymbol.EQ,
                                                                                                                                                                RecAccessExpr(
                                                                                                                                                                    PairAccessExpr(
                                                                                                                                                                        VarExpr(
                                                                                                                                                                            "v1"),
                                                                                                                                                                        0),
                                                                                                                                                                    'p_container'),
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "lgbox"))),
                                                                                                                                                        CompareExpr(
                                                                                                                                                            CompareSymbol.EQ,
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v1"),
                                                                                                                                                                    0),
                                                                                                                                                                'p_container'),
                                                                                                                                                            VarExpr(
                                                                                                                                                                "lgpack"))),
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.EQ,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v1"),
                                                                                                                                                                0),
                                                                                                                                                            'p_container'),
                                                                                                                                                        VarExpr(
                                                                                                                                                            "lgpkg")))),
                                                                                                                                            MulExpr(
                                                                                                                                                CompareExpr(
                                                                                                                                                    CompareSymbol.GTE,
                                                                                                                                                    RecAccessExpr(
                                                                                                                                                        PairAccessExpr(
                                                                                                                                                            VarExpr(
                                                                                                                                                                "v1"),
                                                                                                                                                            0),
                                                                                                                                                        'p_size'),
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        1)),
                                                                                                                                                CompareExpr(
                                                                                                                                                    CompareSymbol.LTE,
                                                                                                                                                    RecAccessExpr(
                                                                                                                                                        PairAccessExpr(
                                                                                                                                                            VarExpr(
                                                                                                                                                                "v1"),
                                                                                                                                                            0),
                                                                                                                                                        'p_size'),
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        15))))),
                                                                                                                                    DicConsExpr(
                                                                                                                                        [
                                                                                                                                            (
                                                                                                                                                RecAccessExpr(
                                                                                                                                                    PairAccessExpr(
                                                                                                                                                        VarExpr(
                                                                                                                                                            "v1"),
                                                                                                                                                        0),
                                                                                                                                                    'p_partkey'),
                                                                                                                                                RecConsExpr(
                                                                                                                                                    [
                                                                                                                                                        (
                                                                                                                                                            'p_brand',
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v1"),
                                                                                                                                                                    0),
                                                                                                                                                                'p_brand')),
                                                                                                                                                        (
                                                                                                                                                            'p_size',
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v1"),
                                                                                                                                                                    0),
                                                                                                                                                                'p_size')),
                                                                                                                                                        (
                                                                                                                                                            'p_container',
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v1"),
                                                                                                                                                                    0),
                                                                                                                                                                'p_container'))]))]),
                                                                                                                                    EmptyDicConsExpr()),
                                                                                                                                True),
                                                                                                                            LetExpr(
                                                                                                                                VarExpr(
                                                                                                                                    "li_probed"),
                                                                                                                                LetExpr(
                                                                                                                                    VarExpr(
                                                                                                                                        "v3"),
                                                                                                                                    VarExpr(
                                                                                                                                        "pa_indexed"),
                                                                                                                                    SumExpr(
                                                                                                                                        VarExpr(
                                                                                                                                            "v4"),
                                                                                                                                        VarExpr(
                                                                                                                                            "db->li_dataset"),
                                                                                                                                        IfExpr(
                                                                                                                                            MulExpr(
                                                                                                                                                CompareExpr(
                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                    RecAccessExpr(
                                                                                                                                                        PairAccessExpr(
                                                                                                                                                            VarExpr(
                                                                                                                                                                "v4"),
                                                                                                                                                            0),
                                                                                                                                                        'l_shipinstruct'),
                                                                                                                                                    VarExpr(
                                                                                                                                                        "deliverinperson")),
                                                                                                                                                AddExpr(
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.EQ,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v4"),
                                                                                                                                                                0),
                                                                                                                                                            'l_shipmode'),
                                                                                                                                                        VarExpr(
                                                                                                                                                            "air")),
                                                                                                                                                    CompareExpr(
                                                                                                                                                        CompareSymbol.EQ,
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v4"),
                                                                                                                                                                0),
                                                                                                                                                            'l_shipmode'),
                                                                                                                                                        VarExpr(
                                                                                                                                                            "airreg")))),
                                                                                                                                            IfExpr(
                                                                                                                                                CompareExpr(
                                                                                                                                                    CompareSymbol.NE,
                                                                                                                                                    DicLookupExpr(
                                                                                                                                                        VarExpr(
                                                                                                                                                            "v3"),
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v4"),
                                                                                                                                                                0),
                                                                                                                                                            'l_partkey')),
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        None)),
                                                                                                                                                IfExpr(
                                                                                                                                                    AddExpr(
                                                                                                                                                        AddExpr(
                                                                                                                                                            MulExpr(
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        DicLookupExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v3"),
                                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                                    VarExpr(
                                                                                                                                                                                        "v4"),
                                                                                                                                                                                    0),
                                                                                                                                                                                'l_partkey')),
                                                                                                                                                                        'p_brand'),
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "brand12")),
                                                                                                                                                                MulExpr(
                                                                                                                                                                    CompareExpr(
                                                                                                                                                                        CompareSymbol.GTE,
                                                                                                                                                                        RecAccessExpr(
                                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                                VarExpr(
                                                                                                                                                                                    "v4"),
                                                                                                                                                                                0),
                                                                                                                                                                            'l_quantity'),
                                                                                                                                                                        ConstantExpr(
                                                                                                                                                                            1)),
                                                                                                                                                                    CompareExpr(
                                                                                                                                                                        CompareSymbol.LTE,
                                                                                                                                                                        RecAccessExpr(
                                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                                VarExpr(
                                                                                                                                                                                    "v4"),
                                                                                                                                                                                0),
                                                                                                                                                                            'l_quantity'),
                                                                                                                                                                        ConstantExpr(
                                                                                                                                                                            11)))),
                                                                                                                                                            MulExpr(
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.EQ,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        DicLookupExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v3"),
                                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                                    VarExpr(
                                                                                                                                                                                        "v4"),
                                                                                                                                                                                    0),
                                                                                                                                                                                'l_partkey')),
                                                                                                                                                                        'p_brand'),
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "brand23")),
                                                                                                                                                                MulExpr(
                                                                                                                                                                    CompareExpr(
                                                                                                                                                                        CompareSymbol.GTE,
                                                                                                                                                                        RecAccessExpr(
                                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                                VarExpr(
                                                                                                                                                                                    "v4"),
                                                                                                                                                                                0),
                                                                                                                                                                            'l_quantity'),
                                                                                                                                                                        ConstantExpr(
                                                                                                                                                                            10)),
                                                                                                                                                                    CompareExpr(
                                                                                                                                                                        CompareSymbol.LTE,
                                                                                                                                                                        RecAccessExpr(
                                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                                VarExpr(
                                                                                                                                                                                    "v4"),
                                                                                                                                                                                0),
                                                                                                                                                                            'l_quantity'),
                                                                                                                                                                        ConstantExpr(
                                                                                                                                                                            20))))),
                                                                                                                                                        MulExpr(
                                                                                                                                                            CompareExpr(
                                                                                                                                                                CompareSymbol.EQ,
                                                                                                                                                                RecAccessExpr(
                                                                                                                                                                    DicLookupExpr(
                                                                                                                                                                        VarExpr(
                                                                                                                                                                            "v3"),
                                                                                                                                                                        RecAccessExpr(
                                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                                VarExpr(
                                                                                                                                                                                    "v4"),
                                                                                                                                                                                0),
                                                                                                                                                                            'l_partkey')),
                                                                                                                                                                    'p_brand'),
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "brand34")),
                                                                                                                                                            MulExpr(
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.GTE,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        PairAccessExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v4"),
                                                                                                                                                                            0),
                                                                                                                                                                        'l_quantity'),
                                                                                                                                                                    ConstantExpr(
                                                                                                                                                                        20)),
                                                                                                                                                                CompareExpr(
                                                                                                                                                                    CompareSymbol.LTE,
                                                                                                                                                                    RecAccessExpr(
                                                                                                                                                                        PairAccessExpr(
                                                                                                                                                                            VarExpr(
                                                                                                                                                                                "v4"),
                                                                                                                                                                            0),
                                                                                                                                                                        'l_quantity'),
                                                                                                                                                                    ConstantExpr(
                                                                                                                                                                        30))))),
                                                                                                                                                    MulExpr(
                                                                                                                                                        RecAccessExpr(
                                                                                                                                                            PairAccessExpr(
                                                                                                                                                                VarExpr(
                                                                                                                                                                    "v4"),
                                                                                                                                                                0),
                                                                                                                                                            'l_extendedprice'),
                                                                                                                                                        SubExpr(
                                                                                                                                                            ConstantExpr(
                                                                                                                                                                1.0),
                                                                                                                                                            RecAccessExpr(
                                                                                                                                                                PairAccessExpr(
                                                                                                                                                                    VarExpr(
                                                                                                                                                                        "v4"),
                                                                                                                                                                    0),
                                                                                                                                                                'l_discount'))),
                                                                                                                                                    ConstantExpr(
                                                                                                                                                        None)),
                                                                                                                                                EmptyDicConsExpr()),
                                                                                                                                            EmptyDicConsExpr()),
                                                                                                                                        False)),
                                                                                                                                LetExpr(
                                                                                                                                    VarExpr(
                                                                                                                                        "results"),
                                                                                                                                    DicConsExpr(
                                                                                                                                        [
                                                                                                                                            (
                                                                                                                                                RecConsExpr(
                                                                                                                                                    [
                                                                                                                                                        (
                                                                                                                                                            'revenue',
                                                                                                                                                            VarExpr(
                                                                                                                                                                "li_probed"))]),
                                                                                                                                                ConstantExpr(
                                                                                                                                                    True))]),
                                                                                                                                    LetExpr(
                                                                                                                                        VarExpr(
                                                                                                                                            "out"),
                                                                                                                                        VarExpr(
                                                                                                                                            "results"),
                                                                                                                                        ConstantExpr(
                                                                                                                                            True)))))))))))))))))))))))
```