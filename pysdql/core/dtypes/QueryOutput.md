# Q1 (Done)
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("li_groupby_agg"), 
        SumExpr(VarExpr("x_li"), 
                VarExpr("db->li_dataset"), 
                IfExpr(CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipdate'), ConstantExpr(19980902)), 
                       DicConsExpr([(RecConsExpr([('l_returnflag', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_returnflag')), 
                                                  ('l_linestatus', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_linestatus'))]), 
                                     RecConsExpr([('sum_qty', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity')), 
                                                  ('sum_base_price', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice')), 
                                                  ('sum_disc_price', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount')))), ('sum_charge', MulExpr(MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))), AddExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_tax')))), 
                                                  ('count_order', ConstantExpr(1))]))]), 
                       EmptyDicConsExpr()), False), 
        LetExpr(VarExpr("li_filter_insert_insert_groupby_agg"), 
                SumExpr(VarExpr("v5"), VarExpr("li_groupby_agg"), DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v5"), 0), PairAccessExpr(VarExpr("v5"), 1)), ConstantExpr(True))]), True), 
                LetExpr(VarExpr("out"), VarExpr("li_filter_insert_insert_groupby_agg"), ConstantExpr(True))))
```

# Q3 (Done)
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("cu_part"), 
        SumExpr(VarExpr("x_cu"), 
                VarExpr("db->cu_dataset"), 
                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_custkey'), 
                              RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_custkey'))]))]), 
                True), LetExpr(VarExpr("cu_ord_join"), 
                               SumExpr(VarExpr("x_ord"), 
                                       VarExpr("db->ord_dataset"), 
                                       IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19950315)), 
                                              IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), ConstantExpr(None)), 
                                                     DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                                                   RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate')), 
                                                                                ('o_shippriority', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_shippriority'))]))]), 
                                                     EmptyDicConsExpr()), 
                                              EmptyDicConsExpr()), 
                                       False), LetExpr(VarExpr("cu_ord_join_li_join"), 
                                                       SumExpr(VarExpr("x_li"), 
                                                               VarExpr("db->li_dataset"), 
                                                               IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipdate'), ConstantExpr(19950315)), 
                                                                      IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                                             DicConsExpr([(RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), 
                                                                                                        ('o_orderdate', RecAccessExpr(DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'o_orderdate')), 'o_orderdate')), 
                                                                                                        ('o_shippriority', RecAccessExpr(DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'o_shippriority')), 'o_shippriority'))]), 
                                                                                           RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                                                             EmptyDicConsExpr()), 
                                                                      EmptyDicConsExpr()), 
                                                               False), LetExpr(VarExpr("result"), SumExpr(VarExpr("v8"), VarExpr("cu_ord_join_li_join"), DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v8"), 0), PairAccessExpr(VarExpr("v8"), 1)), ConstantExpr(True))]), True), 
                                                                               LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True))))))
```

# Q6 (Done)
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("result"), 
        SumExpr(VarExpr("x_li"), 
                VarExpr("db->li_dataset"), 
                DicConsExpr([(RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount')))]), ConstantExpr(True))]), 
                False), 
        LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True)))
```

```python

```

# Q10
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("ord_part"), 
        SumExpr(VarExpr("x_ord"), 
                VarExpr("db->ord_dataset"), 
                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19940101))), 
                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                     RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_custkey')), 
                                                  ('c_name', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_name')), 
                                                  ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_acctbal')), 
                                                  ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_phone')), 
                                                  ('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'n_name')), 
                                                  ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_address')), 
                                                  ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_comment'))]))]), 
                       EmptyDicConsExpr()), 
                True), LetExpr(VarExpr("ord_probe"), 
                               SumExpr(VarExpr("x_ord"), 
                                       VarExpr("db->ord_dataset"), 
                                       IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19940101))), 
                                              IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("ord_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                     DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                                                   RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_custkey')), 
                                                                                ('c_name', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_name')), 
                                                                                ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_acctbal')), 
                                                                                ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_phone')), 
                                                                                ('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'n_name')), 
                                                                                ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_address')), 
                                                                                ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_comment'))]))]), 
                                                     EmptyDicConsExpr()), 
                                              EmptyDicConsExpr()), 
                                       False), LetExpr(VarExpr("li_probe"), 
                                                       SumExpr(VarExpr("x_li"), 
                                                               VarExpr("db->li_dataset"), 
                                                               IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_returnflag'), VarExpr("R")), 
                                                                      IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("db->li_dataset"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                                             DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_custkey')), 'c_custkey')),
                                                                                                        ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_name')), 'c_name')), 
                                                                                                        ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_acctbal')), 'c_acctbal')), 
                                                                                                        ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_phone')), 'c_phone')), 
                                                                                                        ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'n_name')), 'n_name')), 
                                                                                                        ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_address')), 'c_address')), 
                                                                                                        ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("ord_probe"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'c_comment')), 'c_comment'))]), 
                                                                                           RecConsExpr([('sum_revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                                                             EmptyDicConsExpr()), 
                                                                      EmptyDicConsExpr()), 
                                                               False), 
                                                       LetExpr(VarExpr("result"), 
                                                               SumExpr(VarExpr("v5"), 
                                                                       VarExpr("li_probe"), 
                                                                       DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v5"), 0), PairAccessExpr(VarExpr("v5"), 1)), ConstantExpr(True))]), True), LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True))))))
```

# Q19
```python
from pysdql.core.dtypes.sdql_ir import *

LetExpr(VarExpr("pa_part"), 
        SumExpr(VarExpr("x_pa"), 
                VarExpr("db->pa_dataset"), 
                IfExpr(AddExpr(AddExpr(MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand'), VarExpr("Brand")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("SMPKG")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("SMPACK"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("SMCASE"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("SMBOX")))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(5))), MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand'), VarExpr("Brand")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("MEDPACK")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("MEDPKG"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("MEDBAG"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("MEDBOX")))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(10)))), MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand'), VarExpr("Brand")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("LGPKG")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("LGPACK"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("LGCASE"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("LGBOX")))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(15)))), 
                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_partkey'), 
                                     RecConsExpr([('p_brand', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand')), 
                                                  ('p_size', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size')), 
                                                  ('p_container', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'))]))]), 
                       EmptyDicConsExpr()), 
                True), 
        LetExpr(VarExpr("li_probe"), 
                SumExpr(VarExpr("x_li"), 
                        VarExpr("db->li_dataset"), 
                        IfExpr(AddExpr(AddExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'p_brand'), VarExpr("Brand")), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity'), ConstantExpr(1)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity'), ConstantExpr(11)))), MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'p_brand'), VarExpr("Brand")), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity'), ConstantExpr(10)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity'), ConstantExpr(20))))), MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'p_brand'), VarExpr("Brand")), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity'), ConstantExpr(20)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_quantity'), ConstantExpr(30))))), 
                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("pa_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_partkey')), ConstantExpr(None)), 
                                      DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_partkey'), RecConsExpr([('l_partkey', PairAccessExpr(VarExpr("x_li"), 0))]))]), 
                                      EmptyDicConsExpr()), 
                               EmptyDicConsExpr()), 
                        False), 
                ConstantExpr("placeholder_merge_probe_statement")))
```