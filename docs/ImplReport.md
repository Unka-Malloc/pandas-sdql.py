# Python Mode
## General
### Slow Column Projection - Unoptimized (Keep or Remove)
A slow column projection often takes two steps for unoptimized version.

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
## Unoptimized
### Pass
`[1, 4, 5, 9, 12, 13, 16, ]`

### Fail
`[6, 18, ]`

### Error
`[2, 3, 7, 8, 10, 11, 14, 15, 17, 19, 20, 21, 22]`