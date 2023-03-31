# Python Mode
## General
### Slow Column Projection - Unoptimized (Keep or Remove)
A slow column projection often takes two steps for unoptimized version.

### Q5

### Unreasonable Incorrect Result - Unoptimized
```
# PostgreSQL
lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) * (x[0].l_discount >= 0.05))) * (x[0].l_discount <= 0.07))) * (x[0].l_quantity < 24))) else (None))

lineitem_1 = lineitem_0.sum(lambda x: ((x[0].l_extendedprice) * (x[0].l_discount)))

results = {record({"revenue": lineitem_1}): True}
```

```
>>> {[('revenue', 11803420.25340003)]: True}
```

```
# DuckDB Plan
lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) * (x[0].l_discount >= 0.05))) * (x[0].l_discount <= 0.07))) * (x[0].l_quantity < 24))) else (None))

lineitem_attach_to_df_aggr_1 = lineitem_0.sum(lambda x: {x[0].concat(record({"l_extendedpricel_discount": ((x[0].l_extendedprice) * (x[0].l_discount))})): x[1]})

df_aggr_1_0 = lineitem_attach_to_df_aggr_1.sum(lambda x: {record({"l_extendedpricel_discount": x[0].l_extendedpricel_discount}): True})

df_aggr_1_1 = df_aggr_1_0.sum(lambda x: record({"l_extendedpricel_discount": x[0].l_extendedpricel_discount}))

results = {record({"revenue": df_aggr_1_1.l_extendedpricel_discount}): True}
```

```
>>> {[('revenue', 11346352.527700035)]: True}
```

### Specific Query
#### Q13: Not following the TPC-H standard query, our query changes the conditions, even in SDQL.py.

# PostgreSQL
## Difference
- Q13: External Function `LastIndex` required!
- Q15: Replace `max()` with `float`  
    | magic number: 1614410.2928000002 sometimes works (unstable, unpredictable)

## Unoptimized
### Pass
`[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22]`

### Fail
`[21, ]`

### Error
`[]`

# DuckDB

## Incorrect Query: Q15
```
df_group_2 = df_filter_3 \
        .groupby(['supplier_no'], sort=False) \
        .agg(
        total_revenue=("before_1", "sum"),
    )
    df_group_2 = df_group_2[['total_revenue']]
    df_group_2 = df_group_2.reset_index()
    df_aggr_1 = pd.DataFrame()
    df_aggr_1['maxtotal_revenue'] = [(df_group_2.total_revenue).max()]
    df_aggr_1 = df_aggr_1[['maxtotal_revenue']]
    df_limit_1 = df_aggr_1.head(1)
    df_merge_2 = df_merge_1.merge(df_limit_1, left_on=['total_revenue'], right_on=['maxtotal_revenue'], how="inner",
                                  sort=False)
```

## Unoptimized
### Pass
`[1, 3, 4, 5, 9, 10, 12, 13, 16, 19]`

### Fail
`[6, 8, 18, ]`

### Error
`[2, 7, 11, 14, 15, 17, 20, 21]`

## Plan
### Aggr & AggrBinOp -> Rename
`[14, 17]`

### head(1) -> Get As DataFrame
`[11, 15, 22]`

### how='cross' -> Full Outer Join
`[11, 22]`

### TypeError: unsupported operand type(s) for &: 'ColOpIsNull' and 'ColOpExternal'
`[20]`

### Unkown Issues 
- Q2: `AttributeError: 'NoneType' object has no attribute 'sum'`
- Q7: `AttributeError: 'NoneType' object has no attribute 'sum'`
- Q21: `AttributeError: 'NoneType' object has no attribute 'sum'`

### Q21 itself represent issues, always do this last