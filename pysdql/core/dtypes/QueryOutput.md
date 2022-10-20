# Q1 (Done)
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(li_groupby_agg, 
        SumExpr(x_li, 
                db->li_dataset, 
                IfExpr(CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19980902)), 
                       DicConsExpr([(
                           RecConsExpr([('l_returnflag', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_returnflag')), 
                                        ('l_linestatus', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_linestatus'))
                                        ]), 
                           RecConsExpr([('sum_qty', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity')), 
                                        ('sum_base_price', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice')), 
                                        ('sum_disc_price', MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount')))), 
                                        ('sum_charge', MulExpr(MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))), AddExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_tax')))), 
                                        ('count_order', ConstantExpr(1))
                                        ])
                       )]), 
                       ConstantExpr(None)
                       ), 
                False
        ), 
        LetExpr(li_filter_insert_insert_groupby_agg, 
                SumExpr(v2, 
                        li_groupby_agg, 
                        DicConsExpr([(ConcatExpr(PairAccessExpr(v2, 0), PairAccessExpr(v2, 1)), ConstantExpr(True))]), 
                        True
                        ), 
                LetExpr(out, li_filter_insert_insert_groupby_agg, ConstantExpr(True))
        )
)
```

# Q3 (Done)
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(ord_part, 
        SumExpr(x_ord, 
                db->ord_dataset, 
                IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate'), ConstantExpr(19950315)), 
                           DicConsExpr([(RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderkey'), RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate')), ('o_shippriority', RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_shippriority'))]))]), 
                           ConstantExpr(None)), 
                True), 
        LetExpr(ord_probe, 
                SumExpr(x_ord, 
                        db->ord_dataset, 
                        IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate'), ConstantExpr(19950315)), 
                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(ord_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'l_orderkey')), ConstantExpr(None)), 
                                      DicConsExpr([(RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderkey'), 
                                                    RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate')), 
                                                                 ('o_shippriority', RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_shippriority'))]))]), 
                                      EmptyDicConsExpr()), 
                               EmptyDicConsExpr()), 
                        False), 
                LetExpr(li_probe, 
                        SumExpr(x_li, 
                                db->li_dataset, 
                                    IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19950315)), 
                                           IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(db->li_dataset, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), ConstantExpr(None)), 
                                                  DicConsExpr([(
                                                      RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 
                                                                   ('o_orderdate', RecAccessExpr(DicLookupExpr(ord_probe, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate')), 'o_orderdate')), 
                                                                   ('o_shippriority', RecAccessExpr(DicLookupExpr(ord_probe, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_shippriority')), 'o_shippriority'))]), 
                                                      RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))))]))]), 
                                                  EmptyDicConsExpr()), 
                                    EmptyDicConsExpr()), 
                                False), 
                        LetExpr(result, 
                                SumExpr(v5, 
                                        li_probe, 
                                        DicConsExpr([(ConcatExpr(PairAccessExpr(v5, 0), PairAccessExpr(v5, 1)), ConstantExpr(True))]), 
                                        True), 
                                LetExpr(out, result, ConstantExpr(True))
                        )
                )
        )
)
```

# Q6 (Done)
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(result, 
        SumExpr(x_li, 
                db->li_dataset, 
                IfExpr(
                    MulExpr(
                        MulExpr(
                            MulExpr(
                                MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19940101)), 
                                        CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19950101))
                                        ), 
                                CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'), ConstantExpr(0.05))
                            ), 
                            CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'), ConstantExpr(0.07))
                        ), 
                       CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(24))
                    ), 
                    MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount')), 
                    ConstantExpr(0.0)
                ), 
                False
        ),
        LetExpr(out, result, ConstantExpr(True))
)
```