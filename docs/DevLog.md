# Compiler Mode

## 
- [ ] 1 (`Error: div type cannot be deduced!`)
- [ ] 8 (`Error: unkown value type!`)
- [ ] 17 
- [ ] 22

# Unoptimized Plan!

- [x] 1
- [x] 2
- [x] 3
- [x] 4
- [x] 5
- [x] 6
- [x] 7
- [x] 8
- [x] 9
- [x] 10
- [x] 11
- [x] 12
- [ ] 13
- [x] 14
- [ ] 15 (`float` precision probelem) (`max` required)
- [x] 16
- [x] 17
- [x] 18
- [x] 19
- [x] 20
- [x] 21
- [x] 22

# PostgreSQL Plan!

## First Round
- [x] 1
- [ ] 2
- [ ] 3 (`unique` required)
- [ ] 4 (`unique` required)
- [ ] 5 (`unique` required)
- [x] 6
- [ ] 10 (`unique` required)
- [x] 12
- [x] 13
- [ ] 14 (`unique` required)
- [ ] 15 (`max` required)
- [ ] 16 (`unique` required)
- [ ] 19 (`unique` required)

## Second Round
- [ ] 17
- [ ] 18
- [ ] 20

## Third Round
- [ ] 7
- [ ] 8
- [ ] 9
- [ ] 11
- [ ] 21
- [ ] 22

## Unexpected Error
- Q5 possibly primary key issue (l_orderkey, l_lineitem for lineitem)
  ```
  sdqlpy\sdql_lib.py:202: RuntimeWarning: overflow encountered in long_scalars
    result.__container[k] += v
  ```
- Q16: possibly primary key issue (ps_partkey, ps_suppkey for partsupp)

## Anti Join (`outer`, `left_only`)
- [ ] Q21
- [ ] Q22

## Renaming Columns (`a_x`, `a_y`)
- [ ] Q7
- [ ] Q21

## Aggregation Insert (`R["A"] = [(R.B).sum()]`)
- [x] Q6
- [ ] Q11
- [x] Q14
- [ ] Q15
- [ ] Q17
- [x] Q19
- [ ] Q22

## Regex
- [x] Q2
- [x] Q9
- [x] Q13
- [x] Q16
- [x] Q20

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
`l_orderkey` duplicates issue

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
`l_orderkey` duplicates issue

### Callum's Issue To Be Fixed
The result should return 2 columns but got 1.

## Q5
__Unexpected Error__

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

# Refactor Plan!

## Apply
- [x] 8
- [x] 12
- [x] 14
- [x] 17