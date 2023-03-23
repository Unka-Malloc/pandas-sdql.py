# Hyper - Opt - Python
- Finished: [test_hyper_opt_py_01.ipynb](https://github.com/cxunka/pd2sd/blob/ffbdb134b81ef5b82bde395b04fa5beb8108595c/test_hyper_opt_py_01.ipynb)

# Hyper - Opt - Compiler
- Fixed: remove redundant conditions (`?[x[0].probe_key]`)
- Finished: 
  1. [test_hyper_opt](https://github.com/cxunka/pd2sd/blob/ffbdb134b81ef5b82bde395b04fa5beb8108595c/compiler_tests/test_hyper_opt.py)
  2. [results_hyper_opt](https://github.com/cxunka/pd2sd/blob/ffbdb134b81ef5b82bde395b04fa5beb8108595c/compiler_tests/results_hyper_opt.txt)

# Hyper - Unopt - Python
- Finished: [test_hyper_unopt_py_01.ipynb](https://github.com/cxunka/pd2sd/blob/ffbdb134b81ef5b82bde395b04fa5beb8108595c/test_hyper_unopt_py_01.ipynb)

# Hyper - Unopt - Compiler
- Fixed: remove needs for `x[0] != None`
- Finished: [test_hyper_unopt](https://github.com/cxunka/pd2sd/blob/ffbdb134b81ef5b82bde395b04fa5beb8108595c/compiler_tests/test_hyper_unopt.py)

# Duck - Opt - Python

# Duck - Unopt - Python

# Psql - Opt - Python

# Psql - Unopt - Python

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
- [x] 13
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
- [ ] 2 (3. undefined)
- [ ] 3 (`unique` required) (1. bad postgres plan: )
- [ ] 4 (`unique` required) (2. `last` aggregation) (possible solution: equivalent syntax replacement)
- [ ] 5 (`unique` required) (1. bad postgres plan: `region ! nation ! supplier ! lineitem` then another chain to `orders`)
- [x] 6
- [ ] 10 (`unique` required) (2. `last` aggregation) (no alternative) (*mysterious postgres plan*)
- [x] 12
- [x] 13
- [ ] 14 (nothing)
- [ ] 15 (`max` required) (nothing but float precision please) (need some `close to float` or integer promotion)
- [ ] 16 (`unique` required) (the real `nunique` since non primary key)
- [ ] 19 (`unique` required) (unoptimized: if projection is needed, then unique is needed)

## Second Round
- [ ] 17 (7. mean()) (__very bad postgres plan__) (__hidden conflicting attibutes__)
- [ ] 18 (2. `last` aggregation) (__very bad postgres plan__)
- [ ] 20 (__very bad postgres plan__) (__hidden conflicting attibutes__)

## Third Round
- [ ] 7 (1. bad postgres plan)
- [ ] 8 (nothing)
- [ ] 9 (1. bad postgres plan `lineitem <- part` `orders <- lineitem` )
- [ ] 11 (nothing)
- [ ] 21 (1. bad postgres plan & 4. anti-join & 5. unknown optimization plan & 6. unexpected pandas series nested usage)
- [ ] 22 (4. anti-join & 7. mean())

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
