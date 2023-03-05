# PostgreSQL Plan!

## Passed
- Q1
- Q6
- Q12

## Waiting For Aggregation Insert as List
- Q6
- Q11
- Q14
- Q15
- Q17
- Q19
- Q22

## Waiting For Regex
- Q2
- Q9
- Q13
- Q16
- Q20

## Queries That Require `Unique` Function
- Q3
- Q4
- Q6
- Q7
- Q8
- Q9
- Q10

## Q1
### Hesam's Issue To Be Fixed
Add `mean` aggregation for SDQL.py since Callum's code included them.
```
avg_qty=("l_quantity", "mean"),
avg_price=("l_extendedprice", "mean"),
avg_disc=("l_discount", "mean"),
```

## Q3
### Hesam's Issue To Be Fixed
l_orderkey issue

### Callum's Issue To Be Fixed
The result should have 4 columns (including `[l_orderkey, o_orderdate, o_shippriority]`) but not 1 column (only `revenue`).

This can be fixed by adding columns here.
```
# Add [l_orderkey, o_orderdate, o_shippriority]
df_sort_2 = df_sort_2[['revenue']]
df_limit_1 = df_sort_2.head(10)
return df_limit_1
```

## Q4
### Hesam's Issue To Be Fixed
l_orderkey issue

### Callum's Issue To Be Fixed
The result should return 2 columns but got 1.

## Q5
### To Be Fixed
Error?

## Q6
### Hesam's Issue To Be Fixed
l_orderkey issue

## Q7
### Ignore

### Callum's Issue To Be Fixed
The result should return 4 columns but got 1.

## Q8
### Ignore

### Callum's Issue To Be Fixed
The result should return 4 columns but got 1.

## Q9
### Ignore