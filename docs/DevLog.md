Important changes in pysdql With-Inspector branch
1. Table name from 're' to 'region' to avoid name duplication (re is regex package in python)

# Q5
Exceptionally difficult implementation.  
Easy query, hard inference.

# Q7
Columns renaming and multi hash join.

# Q8
Not allowed: Use ConstantExpr(True) as value if empty.
```
DicConsExpr([(RecAccessExpr(PairAccessExpr(x_re, 0), 'r_regionkey'), 
              ConstantExpr(True))])
```

```
DicConsExpr([(RecAccessExpr(PairAccessExpr(v1, 0), 'r_regionkey'), 
              RecConsExpr([('r_regionkey', RecAccessExpr(PairAccessExpr(v1, 0), 'r_regionkey'))]))])
```

Why not probe on `[cu, ord]`?

Why no 
```
CompareExpr(CompareSymbol.NE, 
    DicLookupExpr(re_na_cu, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 
    ConstantExpr(None)
```

Multiple key-value index : `DicLookupExpr(DicLookupExpr(DicLookupExpr())))`

Reconstruct from dict: 

```
('n_nationkey', 
    RecAccessExpr(DicLookupExpr(re_na_cu, RecAccessExpr(PairAccessExpr(x_ord, 0), 'o_custkey')), 'n_nationkey'))
```