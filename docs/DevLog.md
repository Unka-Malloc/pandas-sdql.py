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

# Q1
GroupbyAggrFrame

# Q6
AggrFrame

# Q4
IsInExpr
GroupbyAggrFrame

Error: 
    1. Variable Not Defined (lineitem_part). 
        This could be fixed in GroupbyAggrFrame, by adding context_variables from isin_part to isin_probe

# Q14

Done

# Q16

Done

Error: 
    1. Variable Not Defined (Customer, Complaints). 
        This could be fixed in JointFrame, by adding context_variables from isin_part to isin_probe

# Q15

Done

# Q19

Done

# Q3

Done

# Q10

Done

# Q18

as_part_for_next_join -> probe side is not joint

Done
