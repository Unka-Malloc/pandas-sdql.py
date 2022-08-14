# PySDQL
 
`data_matcher` SDQL -> Python

`data_parser` File -> Python

`data_interpreter` Python -> SDQL

`DataFrame` mutable = False

If DataFrame is created from data, including `DataFrame(data=)` and `read_csv()`

then `mutable = False`

else `mutable = True`

Reason: Other DataFrames are intermediate pipes between input and output. 

`DictEl` and `RecEl`

If `mutable == True`: intermediate step

else if `mutable == False`: data collection

Only `data_matcher.from_expr()` will set `mutable == False`
1. `from_record()`
2. `from_dict()`

Split `expr` and `sdql_expr`

`expr => __str__()`: `str(x) | print(x)`

`sdql_expr => __repr__()`: `repr(x) | eval(x)`

### Abstract Interface

`SemiRing`:
1. `expr`
2. `__str__`
3. `sdql_expr`
4. `__repr__`

## Discussion

### to_csv
1. SDQL external function

OR

2. pysdql.to_csv() with SDQL_HOME env

`SDQL_HOME = "path/to/sdql"`

## Refactor History

#### 2022-08-02

Add `data_matcher`: SDQL -> Python

Add `data_interpreter`: Python -> SDQL

Refactor `RecEl`, `DictEl`

Add `DataFrame`

#### 2022-08-03

Refactor `read_csv()`

Refactor `DataFrame`:

1. `df = DataFrame(data={col1: [], col2: []})`
2. `read_csv(path, names=['col1', 'col2'])`

Add `DataFrameColumns`:
1. `__len__()`
2. `__setitem__()`
3. `__getitem__()`
4. `__delitem__()`
5. `__iter__()`
6. `__contains__()`
7. `__repr__()`
8. `__add__()`
9. `append()`
10. `name`
11. `name.setter`

Refactor `OpExpr`:
1. `name`

Add `OpSeq`:
1. `self.stack`
2. `self.names`
3. `pop()`
4. `push()`
5. `peak()`
6. `size`
7. `expr`
8. `__repr__()`

#### 2022-08-04
Refactor `DataFrame.name`
Refactor `VarExpr, ValExpr`
Refactor `IterEl, IterExpr`
Refactor `ColEl, ColExpr`

#### 2022-08-05
Refactor `ValExpr`

Refactor `ColEl`

#### 2022-08-06
Refactor `DataFrame`

Add `DataFrameStruct`:

1. `1DT` Singleton Dictionary: 

`{<a_i = v_i> -> 1}`

2. `LRT` Left Join: 

`{<left = <l_i=lv_i>, right = <r_i=rv_i>> -> v}`

3. `GRP` Group By:

`{<a_i = v_i> -> {k -> v}}`



Refactor `ColEl`

#### 2022-08-07
Refactor `DataFrame`
1. `__str__`
2. `__repr__`

Refactor `ColEl`:
1. `sum()`
2. `count()`
3. `mean()`
4. `min()`
5. `max()`

#### 2022-08-08
Refactor `ColEl`:

1. `from_1DT`
2. `from_LRT`
3. `from_GRP`

4. `key`
5. `val`

6. `sum, count, mean, min, max` 
`return` `ValExpr(operations)`