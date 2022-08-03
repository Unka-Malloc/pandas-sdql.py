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