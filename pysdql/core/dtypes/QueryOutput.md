# Q1 (Done)
```python
li = VarExpr('db->li_dataset')
x_li = VarExpr('x_li')
li_groupby_agg = VarExpr('li_groupby_agg')
li_groupby_agg_concat = VarExpr('li_groupby_agg_concat')
x_li_groupby_agg = VarExpr('x_li_groupby_agg')
out = VarExpr('out')
query = LetExpr(li_groupby_agg, 
                SumExpr(x_li, 
                        li, 
                        IfExpr(CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19980902)), 
                               DicConsExpr([(RecConsExpr([('l_returnflag', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_returnflag')), 
                                                          ('l_linestatus', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_linestatus'))]), 
                                             RecConsExpr([('sum_qty', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity')), 
                                                          ('sum_base_price', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice')), 
                                                          ('sum_disc_price', MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount')))), 
                                                          ('sum_charge', MulExpr(MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))), AddExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_tax')))), 
                                                          ('count_order', ConstantExpr(1))]))]), 
                               EmptyDicConsExpr()), 
                        False), LetExpr(li_groupby_agg_concat, 
                                        SumExpr(x_li_groupby_agg, 
                                                li_groupby_agg, 
                                                DicConsExpr([(ConcatExpr(PairAccessExpr(x_li_groupby_agg, 0), PairAccessExpr(x_li_groupby_agg, 1)), ConstantExpr(True))]), 
                                                True), 
                                        LetExpr(out, li_groupby_agg_concat, ConstantExpr(True))))
```

# Q3 (Done)
```python
cu = VarExpr('db->cu_dataset')
x_cu = VarExpr('x_cu')
building = VarExpr('building')
cu_part = VarExpr('cu_part')
ord = VarExpr('db->ord_dataset')
x_ord = VarExpr('x_ord')
ord_part = VarExpr('ord_part')
cu_ord = VarExpr('cu_ord')
x_cu_ord = VarExpr('x_cu_ord')
li = VarExpr('db->li_dataset')
x_li = VarExpr('x_li')
li_part = VarExpr('li_part')
cu_ord_li = VarExpr('cu_ord_li')
x_cu_ord_li = VarExpr('x_cu_ord_li')
cu_ord_li_groupby_agg = VarExpr('cu_ord_li_groupby_agg')
cu_ord_li_groupby_agg_concat = VarExpr('cu_ord_li_groupby_agg_concat')
x_cu_ord_li_groupby_agg = VarExpr('x_cu_ord_li_groupby_agg')
out = VarExpr('out')

query = LetExpr(building, 
                ConstantExpr("BUILDING"), 
                LetExpr(cu_part, 
                        SumExpr(x_cu, 
                                cu, 
                                IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_mktsegment'), building), 
                                       DicConsExpr([(RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_custkey'), 
                                                     RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_custkey'))]))]), 
                                       EmptyDicConsExpr()), 
                                True), 
                        LetExpr(cu_ord, 
                                SumExpr(x_ord, 
                                        ord, 
                                        IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate'), ConstantExpr(19950315)), 
                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), ConstantExpr(None)), 
                                                      DicConsExpr([(RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderkey'), 
                                                                    RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate')), 
                                                                                 ('o_shippriority', RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_shippriority'))]))]), 
                                                      EmptyDicConsExpr()), 
                                               EmptyDicConsExpr()), 
                                        True), 
                                LetExpr(cu_ord_li, 
                                        SumExpr(x_li, 
                                                li, 
                                                IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19950315)), 
                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), ConstantExpr(None)), 
                                                              DicConsExpr([(RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 
                                                                                         ('o_orderdate', RecAccessExpr(DicLookupExpr(cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'o_orderdate')), 
                                                                                         ('o_shippriority', RecAccessExpr(DicLookupExpr(cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'o_shippriority'))]), 
                                                                            RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))))]))]), 
                                                              EmptyDicConsExpr()), 
                                                       EmptyDicConsExpr()), 
                                                False), 
                                        LetExpr(out, SumExpr(x_cu_ord_li, 
                                                             cu_ord_li, 
                                                             DicConsExpr([(ConcatExpr(PairAccessExpr(x_cu_ord_li, 0), PairAccessExpr(x_cu_ord_li, 1)), ConstantExpr(True))]), 
                                                             True), 
                                                ConstantExpr(True))))))
```

# Q6 (Done)
```python
li = VarExpr('db->li_dataset')
x_li = VarExpr('x_li')
out = VarExpr('out')
query = LetExpr(out, 
                SumExpr(x_li, 
                        li, 
                        IfExpr(MulExpr(MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19940101)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19950101))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'), ConstantExpr(0.05))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'), ConstantExpr(0.07))), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(24))),
                               DicConsExpr([('revenue', 
                                             MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount')))]), 
                               EmptyDicConsExpr()), 
                        False), 
                ConstantExpr(True))
```

# Q10
```python
na = VarExpr('db->na_dataset')
x_na = VarExpr('x_na')
na_part = VarExpr('na_part')
cu = VarExpr('db->cu_dataset')
x_cu = VarExpr('x_cu')
cu_part = VarExpr('cu_part')
ord = VarExpr('db->ord_dataset')
x_ord = VarExpr('x_ord')
ord_part = VarExpr('ord_part')
cu_ord = VarExpr('cu_ord')
x_cu_ord = VarExpr('x_cu_ord')
na_cu_ord = VarExpr('na_cu_ord')
x_na_cu_ord = VarExpr('x_na_cu_ord')
li = VarExpr('db->li_dataset')
x_li = VarExpr('x_li')
li_part = VarExpr('li_part')
r = VarExpr('r')
na_cu_ord_li = VarExpr('na_cu_ord_li')
x_na_cu_ord_li = VarExpr('x_na_cu_ord_li')
x_na_cu_ord_li_groupby_agg = VarExpr('x_na_cu_ord_li_groupby_agg')
out = VarExpr('out')

query = LetExpr(r, ConstantExpr("R"), 
                LetExpr(na_part, 
                        SumExpr(x_na, 
                                na, 
                                DicConsExpr([(RecAccessExpr(PairAccessExpr(x_na, 0), 'n_nationkey'), 
                                              RecConsExpr([('n_name', RecAccessExpr(PairAccessExpr(x_na, 0), 'n_name'))]))]), 
                                True), 
                        LetExpr(cu_part, 
                                SumExpr(x_cu, 
                                        cu, 
                                        DicConsExpr([(RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_custkey'), 
                                                      RecConsExpr([('c_name', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_name')),
                                                                   ('c_acctbal', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_acctbal')), 
                                                                   ('c_address', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_address')), 
                                                                   ('c_nationkey', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_nationkey')),
                                                                   ('c_phone', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_phone')), 
                                                                   ('c_comment', RecAccessExpr(PairAccessExpr(x_cu, 0), 'c_comment'))]))]),
                                        True), 
                                LetExpr(cu_ord,
                                        SumExpr(x_ord, 
                                                ord, 
                                                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderdate'), ConstantExpr(19940101))), 
                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), ConstantExpr(None)), 
                                                              DicConsExpr([(RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_orderkey'), 
                                                                            RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_custkey')),
                                                                                         ('c_name', RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_name')), 
                                                                                         ('c_acctbal', RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_acctbal')), 
                                                                                         ('c_address', RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_address')),
                                                                                         ('c_phone', RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_phone')), 
                                                                                         ('c_comment', RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_comment')),
                                                                                         ('n_name', RecAccessExpr(DicLookupExpr(na_part, RecAccessExpr(DicLookupExpr(cu_part, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'c_nationkey')), 'n_name'))]))]),
                                                              EmptyDicConsExpr()),
                                                       EmptyDicConsExpr()), 
                                                False), 
                                        LetExpr(na_cu_ord_li, 
                                                SumExpr(x_li, 
                                                        li, 
                                                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_returnflag'), r), 
                                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), ConstantExpr(None)), 
                                                                      DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'c_custkey')),
                                                                                                 ('c_name', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'c_name')), 
                                                                                                 ('c_acctbal', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'c_acctbal')), 
                                                                                                 ('c_phone', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'c_phone')), 
                                                                                                 ('n_name', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'n_name')),
                                                                                                 ('c_address', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'c_address')), 
                                                                                                 ('c_comment', RecAccessExpr(DicLookupExpr(na_cu_ord, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_orderkey')), 'c_comment'))]), 
                                                                                    RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))))]))]),
                                                                      EmptyDicConsExpr()),
                                                               EmptyDicConsExpr()), 
                                                        False), 
                                                LetExpr(out, 
                                                        SumExpr(x_na_cu_ord_li, 
                                                                na_cu_ord_li, 
                                                                DicConsExpr([(ConcatExpr(PairAccessExpr(x_na_cu_ord_li, 0), PairAccessExpr(x_na_cu_ord_li, 1)), ConstantExpr(True))]),
                                                                True), 
                                                        ConstantExpr(True)))))))
```

# Q15
```python
su = VarExpr('db->su_dataset')
x_su = VarExpr('x_su')
su_part = VarExpr('su_part')
li = VarExpr('db->li_dataset')
x_li = VarExpr('x_li')
li_part = VarExpr('li_part')
x_li_part = VarExpr('x_li_part')
out = VarExpr('out')

query = LetExpr(li_part, 
                SumExpr(x_li, 
                        li, 
                        IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19960101)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipdate'), ConstantExpr(19960401))), 
                               DicConsExpr([(RecConsExpr([('l_suppkey', 
                                                           RecAccessExpr(PairAccessExpr(x_li, 0), 'l_suppkey'))]), 
                                             RecConsExpr([('total_revenue', 
                                                           MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))))]))]), 
                               EmptyDicConsExpr()), 
                        False), 
                LetExpr(su_part, 
                        SumExpr(x_su, 
                                su, 
                                DicConsExpr([(RecAccessExpr(PairAccessExpr(x_su, 0), 's_suppkey'), 
                                              RecConsExpr([('s_name', RecAccessExpr(PairAccessExpr(x_su, 0), 's_name')), 
                                                           ('s_address', RecAccessExpr(PairAccessExpr(x_su, 0), 's_address')), 
                                                           ('s_phone', RecAccessExpr(PairAccessExpr(x_su, 0), 's_phone'))]))]),
                                True), 
                        LetExpr(out, 
                                SumExpr(x_li_part, 
                                        li_part, 
                                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_li_part, 1), 'total_revenue'), ConstantExpr(1772627.2087)), 
                                               DicConsExpr([(RecConsExpr([('s_suppkey', RecAccessExpr(PairAccessExpr(x_li_part, 0), 'l_suppkey')), 
                                                                          ('s_name', RecAccessExpr(DicLookupExpr(su_part, RecAccessExpr(PairAccessExpr(x_li_part, 0), 'l_suppkey')), 's_name')), 
                                                                          ('s_address', RecAccessExpr(DicLookupExpr(su_part, RecAccessExpr(PairAccessExpr(x_li_part, 0), 'l_suppkey')), 's_address')), 
                                                                          ('s_phone', RecAccessExpr(DicLookupExpr(su_part, RecAccessExpr(PairAccessExpr(x_li_part, 0), 'l_suppkey')), 's_phone')), 
                                                                          ('total_revenue', RecAccessExpr(PairAccessExpr(x_li_part, 1), 'total_revenue'))]),
                                                             ConstantExpr(True))]), 
                                               EmptyDicConsExpr()), 
                                        True), 
                                ConstantExpr(True))))
```

# Q19
```python
pa = VarExpr('db->pa_dataset')
x_pa = VarExpr('x_pa')
pa_part = VarExpr('pa_part')
brand12 = VarExpr('brand12')
smcase = VarExpr('smcase')
smbox = VarExpr('smbox')
smpack = VarExpr('smpack')
smpkg = VarExpr('smpkg')
brand23 = VarExpr('brand23')
medbag = VarExpr('medbag')
medbox = VarExpr('medbox')
medpkg = VarExpr('medpkg')
medpack = VarExpr('medpack')
brand34 = VarExpr('brand34')
lgcase = VarExpr('lgcase')
lgbox = VarExpr('lgbox')
lgpack = VarExpr('lgpack')
lgpkg = VarExpr('lgpkg')
li = VarExpr('db->li_dataset')
x_li = VarExpr('x_li')
li_part = VarExpr('li_part')
air = VarExpr('air')
airreg = VarExpr('airreg')
deliverinperson = VarExpr('deliverinperson')
pa_li = VarExpr('pa_li')
x_pa_li = VarExpr('x_pa_li')
out = VarExpr('out')

query = LetExpr(brand12, ConstantExpr("Brand#12"), 
                LetExpr(smcase, ConstantExpr("SM CASE"), 
                        LetExpr(smbox, ConstantExpr("SM BOX"), 
                                LetExpr(smpack, ConstantExpr("SM PACK"), 
                                        LetExpr(smpkg, ConstantExpr("SM PKG"), 
                                                LetExpr(brand23, ConstantExpr("Brand#23"), 
                                                        LetExpr(medbag, ConstantExpr("MED BAG"),
                                                                LetExpr(medbox, ConstantExpr("MED BOX"),
                                                                        LetExpr(medpkg, ConstantExpr("MED PKG"), 
                                                                                LetExpr(medpack, ConstantExpr("MED PACK"), 
                                                                                        LetExpr(brand34, ConstantExpr("Brand#34"),
                                                                                                LetExpr(lgcase, ConstantExpr("LG CASE"), 
                                                                                                        LetExpr(lgbox, ConstantExpr("LG BOX"), 
                                                                                                                LetExpr(lgpack, ConstantExpr("LG PACK"), 
                                                                                                                        LetExpr(lgpkg, ConstantExpr("LG PKG"),
                                                                                                                                LetExpr(air, ConstantExpr("AIR"),
                                                                                                                                        LetExpr(airreg, ConstantExpr("AIR REG"), 
                                                                                                                                                LetExpr(deliverinperson, ConstantExpr("DELIVER IN PERSON"), 
                                                                                                                                                        LetExpr(pa_part, 
                                                                                                                                                                SumExpr(x_pa, 
                                                                                                                                                                        pa, 
                                                                                                                                                                        IfExpr(AddExpr(AddExpr(MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_brand'), brand12), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), smpkg), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), smpack)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), smcase)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), smbox))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size'), ConstantExpr(5))), MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_brand'), brand23), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), medpack), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), medpkg)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), medbag)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), medbox))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size'), ConstantExpr(10)))), MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_brand'), brand34), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), lgpkg), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), lgpack)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), lgcase)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'), lgbox))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size'), ConstantExpr(15)))), 
                                                                                                                                                                               DicConsExpr([(RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_partkey'), 
                                                                                                                                                                                             RecConsExpr([('p_brand', RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_brand')), 
                                                                                                                                                                                                          ('p_size', RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_size')),
                                                                                                                                                                                                          ('p_container', RecAccessExpr(PairAccessExpr(x_pa, 0), 'p_container'))]))]),
                                                                                                                                                                               EmptyDicConsExpr()), 
                                                                                                                                                                        True),
                                                                                                                                                                LetExpr(pa_li, 
                                                                                                                                                                        SumExpr(x_li, 
                                                                                                                                                                                li,
                                                                                                                                                                                IfExpr(MulExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipmode'), air), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipmode'), airreg)), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_shipinstruct'), deliverinperson)), 
                                                                                                                                                                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(pa_part, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_partkey')), ConstantExpr(None)), 
                                                                                                                                                                                              IfExpr(AddExpr(AddExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(DicLookupExpr(pa_part, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_partkey')), 'p_brand'), brand12), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(1)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(11)))), MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(DicLookupExpr(pa_part, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_partkey')), 'p_brand'), brand23), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(10)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(20))))), MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(DicLookupExpr(pa_part, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_partkey')), 'p_brand'), brand34), MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(20)), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(x_li, 0), 'l_quantity'), ConstantExpr(30))))), 
                                                                                                                                                                                                     DicConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(x_li, 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(x_li, 0), 'l_discount'))))]), 
                                                                                                                                                                                                     EmptyDicConsExpr()), 
                                                                                                                                                                                              EmptyDicConsExpr()), 
                                                                                                                                                                                       EmptyDicConsExpr()), 
                                                                                                                                                                                False), 
                                                                                                                                                                        LetExpr(out, SumExpr(x_pa_li, 
                                                                                                                                                                                             pa_li, 
                                                                                                                                                                                             DicConsExpr([(ConcatExpr(PairAccessExpr(x_pa_li, 0), PairAccessExpr(x_pa_li, 1)), ConstantExpr(True))]), 
                                                                                                                                                                                             True), 
                                                                                                                                                                                ConstantExpr(True))))))))))))))))))))))
```