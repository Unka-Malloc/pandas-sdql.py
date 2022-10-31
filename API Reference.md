## Definition, Access and Binding
### Variable Definition
`x`

`VarExpr('x')`

### Constant Value Definition
`v`

`ConstantExpr(v)`

### Dictionary Construction
`{k1 -> v1, k2 -> v2, k3 -> v3}`

`DicConsExpr([(k1, v1), (k2, v2), (k3, v3)])`

### Dictionary Access
`dict(k)`

`DicLookupExpr(dict, k)`

### Record Construction 
`<a1 = v1, a2 = v2, a3 = v3>`

`RecConsExpr([(a1, v1), (a2, v2), (a3, v3)])`

### Record Access
`rec.a`

`RecAccessExpr(rec, 'a')`

### Dictionary Key-Value Pair Access
#### Key Access
`x.key`

`PairAccessExpr(x, 0)`

#### Val Access
`x.val`

`PairAccessExpr(x, 1)`

### Let Binding 
`let v = var_expr main_expr`

`LetExpr(VarExpr('v'), var_expr, main_expr)`

## Arithmetic Operations

### Addition
`a + b`

`AddExpr(a, b)`

### Multiplication
`a * b`

`MulExpr(a, b)`

### Subtraction
`a - b`

`SubExpr(a, b)`

### Division
`a / b`

`DivExpr(a, b)`

## Comparison Operations

### Equal
`a == b`

`CompareExpr(CompareSymbol.EQ, a, b)`
### Not Equal
`a != b`

`CompareExpr(CompareSymbol.NE, a, b)`

### Less Than
`a < b`

`CompareExpr(CompareSymbol.LT, a, b)`
### Less Than and Equal
`a <= b`

`CompareExpr(CompareSymbol.LTE, a, b)`
### Greater Than
`a > b`

`CompareExpr(CompareSymbol.GT, a, b)`
### Greater Than and Equal
`a >= b`

`CompareExpr(CompareSymbol.GTE, a, b)`

## SDQL IR Without Dependence
### Constant Value
- `ConstantExpr('x')`

### Comparison Symbol
- `CompareSymbol.EQ`
- `CompareSymbol.NE`
- `CompareSymbol.LT`
- `CompareSymbol.LTE`
- `CompareSymbol.GT`
- `CompareSymbol.GTE`

## Some Properties

`DataFrame`
- `name` The name of the DataFrame
- `data` The data of the DataFrame
- `columns` The name of columns of the DataFrame
- `dtype` The data type of columns of the DataFrame

`ColEl`
- `relation | R: DataFrame` 
- `field: str` name of column
- `col: RecAccessExpr`

`CondExpr`
- ``

## What can we do?
### Only One Iteration For Each DataFrame

### The Iteration Element is a Hash Value of The DataFrame

### Fix Independent IR Expr
1. Iteration Element: `iter_el`
   1. The `iter_el` is designed for `pandas.DataFrame`
   2. There is a unique `iter_el` for each `pandas.DataFrame`
   3. __Discussion__: Whether to define the `iter_el` as `str` or `VarExpr(str)`

#### Discussion 1: `str` or `VarExpr(str)`
If the iteration element is defined as string, it will be passed as a string through operations.

String Pros:
- It is a string.

String Cons:
- It does not contain any other information.

What should be contained in an iteration element?
- The dictionary it iterates on.
- The key of the iteration element.
- The value of the iteration element.

Can we use `VarExpr(x)` to represent a `iter_el`?
- __NO__, `VarExpr(x)` does not contains the information provided above.
- Two methods:
  1. Rewrite `VarExpr`
  2. Add a layer over the `VarExpr`

Therefore, `IterEl` is a neccessary design, which represent the iteration element over the dictionary.

`IterEl`
- `name: str` The name of the iteration element
- `el: VarExpr` The variable which represents the iteration element
- `key: Expr` | `k: Expr` Key
- `value: Expr` | `val: Expr` | `v: Expr` Value
- `sdql_ir` Inherit from `SDQLIR`, returns an `sdql_ir` object

4. `CondExpr`
   1. Dependent Part: Record Access
   2. Constant Part: ConstantExpr(). The constant value is given by the user.
   3. Comparison: 

## Operations Before Merge
1. Row Selection with Conditionals
2. Column Projection
3. Column Rename
4. Column Insertion
5. Column Deletion

# Merge
- LeftPart
```python
from pysdql.core.dtypes.sdql_ir import (
    IfExpr,
    ConstantExpr,
    LetExpr,
    VarExpr
)

v1 = VarExpr('')

cond = IfExpr(
    condExpr=ConstantExpr(None),
    thenBodyExpr=ConstantExpr(None),
    elseBodyExpr=ConstantExpr(None)
)

LetExpr(
    varExpr=,
    valExpr=,
    bodyExpr=
)

```
- RightProbe
```python

```

# Empty Dict

# isAssignSum
Concat / BuildPartition -> True
JoinProbe -> False

d = {a -> 1,
 b -> 2}

sum(x in d) x.val

a -> 1,

out = 1

b -> 2

out = 1 + 2

out = 3

This is the case of updating. 

==============

d = {a -> 1,
 b -> 2}

sum(x in d) x.val

a -> 1,

out = 1

b -> 2

out = 2

This is the case of assignment. 

===============

sum(r <- R)
{<r.key, r.val> -> true}

[comment]: <> (concat&#40;<a = 1>, <b = 2>&#41;)

<a=1, b=1> -> true # unique key

<a=2, b=2> -> true

==============
sum(c <- C)
{c.key.city -> 1}

{NY -> 100, 
PA -> 1222}

update:

NY -> 1

NY -> 5

=> NY -> 6

assign:

NY -> 1

NY -> 5 (Ignore)

=> NY -> 1

When the key is unique: 

after df.drop_diplicates()

Partition: assignment = True

we only use primary key (left_on)

Probe: update

# Requirements
1. `__or__` for MulExpr() and AddExpr()

{<revenue = ?> -> True}

# Merge
`is_join_partition_side` >> `join_partition_info`  
`is_join_probe_side` >> `join_probe_info`  
`is_joint` >> `joint_info`  

            'partition_side': None,
            'partition_key': None,

            'probe_side': None,
            'probe_key': None,

            'how': None,

```
self.merge_stmt

self.merge_groupby_agg_stmt
```

```python
LetExpr(VarExpr("ord_probe"), 
        SumExpr(VarExpr("x_ord"), 
                VarExpr("db->ord_dataset"), 
                IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19950315)), 
                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), ConstantExpr(None)), 
                              DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey'), 
                                            RecConsExpr([('o_orderkey', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey')), 
                                                         ('o_orderdate', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate')), 
                                                         ('o_shippriority', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_shippriority'))]))]), 
                              EmptyDicConsExpr()), 
                       EmptyDicConsExpr()), 
                False), 
        ConstantExpr(None))
```

```python
LetExpr(VarExpr("li_probe"), 
        SumExpr(VarExpr("x_li"), 
                VarExpr("db->li_dataset"), 
                IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipdate'), ConstantExpr(19950315)), 
                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                              DicConsExpr([(RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), 
                                                         ('o_orderdate', RecAccessExpr(DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'o_orderdate')), 'o_orderdate')), 
                                                         ('o_shippriority', RecAccessExpr(DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'o_shippriority')), 'o_shippriority'))]), 
                                            RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                              EmptyDicConsExpr()), 
                       EmptyDicConsExpr()), 
                False), 
        LetExpr(VarExpr("result"), 
                SumExpr(VarExpr("v8"), VarExpr("li_probe"), DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v8"), 0), PairAccessExpr(VarExpr("v8"), 1)), ConstantExpr(True))]), True), 
                LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True))))
```

```python
LetExpr(VarExpr("cu_part"), 
        SumExpr(VarExpr("x_cu"), 
                VarExpr("db->cu_dataset"), 
                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_custkey'), 
                              RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_custkey'))]))]), 
                True), LetExpr(VarExpr("cu_ord_join"), 
                               SumExpr(VarExpr("x_ord"), 
                                       VarExpr("db->ord_dataset"), 
                                       IfExpr(CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19950315)), 
                                              IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), ConstantExpr(None)), 
                                                     DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                                                   RecConsExpr([('o_orderdate', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate')), 
                                                                                ('o_shippriority', RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_shippriority'))]))]), 
                                                     EmptyDicConsExpr()), 
                                              EmptyDicConsExpr()), 
                                       False), LetExpr(VarExpr("cu_ord_join_li_join"), 
                                                       SumExpr(VarExpr("x_li"), 
                                                               VarExpr("db->li_dataset"), 
                                                               IfExpr(CompareExpr(CompareSymbol.GT, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipdate'), ConstantExpr(19950315)), 
                                                                      IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                                             DicConsExpr([(RecConsExpr([('l_orderkey', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), 
                                                                                                        ('o_orderdate', RecAccessExpr(DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'o_orderdate')), 'o_orderdate')), 
                                                                                                        ('o_shippriority', RecAccessExpr(DicLookupExpr(VarExpr("cu_ord_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'o_shippriority')), 'o_shippriority'))]), 
                                                                                           RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                                                             EmptyDicConsExpr()), 
                                                                      EmptyDicConsExpr()), 
                                                               False), LetExpr(VarExpr("result"), SumExpr(VarExpr("v8"), VarExpr("cu_ord_join_li_join"), DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v8"), 0), PairAccessExpr(VarExpr("v8"), 1)), ConstantExpr(True))]), True), 
                                                                               LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True))))))
```

```python
LetExpr(VarExpr("pa_part"), 
        SumExpr(VarExpr("x_pa"), 
                VarExpr("db->pa_dataset"), 
                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_partkey'), 
                              RecConsExpr([('p_partkey', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_partkey')), 
                                           ('p_brand', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand')), 
                                           ('p_size', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size')), 
                                           ('p_container', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'))]))]), 
                True), 
        LetExpr(VarExpr("pa_li_join"), 
                SumExpr(VarExpr("x_li"), 
                        VarExpr("db->li_dataset"), 
                        IfExpr(MulExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipmode'), VarExpr("air")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipmode'), VarExpr("airreg"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipinstruct'), VarExpr("deliverinperson"))), 
                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("pa_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_partkey')), ConstantExpr(None)), 
                                      DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_partkey'), 
                                                    RecConsExpr([('l_partkey', RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_partkey'))]))]), 
                                      EmptyDicConsExpr()), 
                               EmptyDicConsExpr()), 
                        False), 
                ConstantExpr("placeholder_probe_next")))
```

```python
LetExpr(VarExpr("pa_part"), 
        SumExpr(VarExpr("x_pa"), 
                VarExpr("db->pa_dataset"), 
                IfExpr(AddExpr(AddExpr(MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand'), VarExpr("brand12")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("smpkg")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("smpack"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("smcase"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("smbox")))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(5))), MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand'), VarExpr("brand23")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("medpack")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("medpkg"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("medbag"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("medbox")))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(10)))), MulExpr(MulExpr(MulExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand'), VarExpr("brand34")), AddExpr(AddExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("lgpkg")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("lgpack"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("lgcase"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'), VarExpr("lgbox")))), CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(1))), CompareExpr(CompareSymbol.LTE, RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size'), ConstantExpr(15)))), 
                       DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_partkey'),
                                     RecConsExpr([('p_brand', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_brand')), 
                                                  ('p_size', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_size')), 
                                                  ('p_container', RecAccessExpr(PairAccessExpr(VarExpr("x_pa"), 0), 'p_container'))]))]), 
                       EmptyDicConsExpr()), 
                True), LetExpr(VarExpr("pa_li_join"), 
                               SumExpr(VarExpr("x_li"), 
                                       VarExpr("db->li_dataset"), 
                                       IfExpr(MulExpr(AddExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipmode'), VarExpr("air")), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipmode'), VarExpr("airreg"))), CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_shipinstruct'), VarExpr("deliverinperson"))), 
                                              IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("pa_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_partkey')), ConstantExpr(None)), 
                                                     DicConsExpr([(RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]), ConstantExpr(True))]), 
                                                     EmptyDicConsExpr()), 
                                              EmptyDicConsExpr()), 
                                       False), 
                               LetExpr(VarExpr("out"), VarExpr("pa_li_join"), ConstantExpr(True))))
```

```python
LetExpr(VarExpr("na_part"), 
        SumExpr(VarExpr("x_na"), 
                VarExpr("na"), 
                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_nationkey'), 
                              RecConsExpr([('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_name'))]))]), 
                True), 
        LetExpr(VarExpr("na_cu_ord_join_join"), 
                SumExpr(VarExpr("x_cu_ord_join"), 
                        VarExpr("cu_ord_join"), 
                        IfExpr(ConstantExpr(None), 
                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("na_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_nationkey')), ConstantExpr(None)), 
                                      DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                                    RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord_join_join"), 0), 'c_custkey')), 
                                                                 ('c_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord_join_join"), 0), 'c_name')), 
                                                                 ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord_join_join"), 0), 'c_acctbal')), 
                                                                 ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord_join_join"), 0), 'c_phone')), 
                                                                 ('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_name')), 
                                                                 ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord_join_join"), 0), 'c_address')), 
                                                                 ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord_join_join"), 0), 'c_comment'))]))]), 
                                      EmptyDicConsExpr()), 
                               EmptyDicConsExpr()), 
                        False), 
                LetExpr(VarExpr("na_cu_ord_join_join_li_join"), 
                        SumExpr(VarExpr("x_li"), 
                                VarExpr("db->li_dataset"), 
                                IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_returnflag'), ConstantExpr("R")), 
                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                              DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_custkey')), 'c_custkey')), 
                                                                         ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_name')), 'c_name')), 
                                                                         ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_acctbal')), 'c_acctbal')), 
                                                                         ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_phone')), 'c_phone')), 
                                                                         ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'n_name')), 'n_name')), 
                                                                         ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_address')), 'c_address')), 
                                                                         ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord_join_join"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_comment')), 'c_comment'))]), 
                                                            RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                              EmptyDicConsExpr()), 
                                       EmptyDicConsExpr()), 
                                False), 
                        LetExpr(VarExpr("result"), 
                                SumExpr(VarExpr("v8"), 
                                        VarExpr("na_cu_ord_join_join_li_join"), 
                                        DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v8"), 0), PairAccessExpr(VarExpr("v8"), 1)), ConstantExpr(True))]), True), LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True))))))
```

```python
LetExpr(VarExpr("cu_ord"), 
        SumExpr(VarExpr("x_ord"), 
                VarExpr("db->ord_dataset"), 
                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19940101))), 
                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), ConstantExpr(None)), 
                              DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                            RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_custkey')), 
                                                         ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_name')), 
                                                         ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_acctbal')), 
                                                         ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_address')), 
                                                         ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_phone')), 
                                                         ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_comment')), 
                                                         ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_part"), RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_nationkey')), 'n_name'))]))]), 
                              EmptyDicConsExpr()), 
                       EmptyDicConsExpr()), 
                False), LetExpr(VarExpr("na_cu_ord_li"), 
                                SumExpr(VarExpr("x_li"), 
                                        VarExpr("db->li_dataset"), 
                                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_returnflag'), ConstantExpr("R")), 
                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                      DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_custkey')), 'c_custkey')), 
                                                                                 ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_name')), 'c_name')), 
                                                                                 ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_acctbal')), 'c_acctbal')), 
                                                                                 ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_phone')), 'c_phone')), 
                                                                                 ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'n_name')), 'n_name')), 
                                                                                 ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_address')), 'c_address')), 
                                                                                 ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_comment')), 'c_comment'))]), 
                                                                    RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                                      EmptyDicConsExpr()), 
                                               EmptyDicConsExpr()), 
                                        False), LetExpr(VarExpr("result"), 
                                                        SumExpr(VarExpr("v21"), VarExpr("na_cu_ord_li"), DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v21"), 0), PairAccessExpr(VarExpr("v21"), 1)), ConstantExpr(True))]), True), 
                                                        LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True)))))
```

```python
LetExpr(VarExpr("na_cu_ord"), 
        SumExpr(VarExpr("x_na_cu_ord"), 
                VarExpr("na_cu_ord"), 
                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'o_orderkey'), 
                              RecConsExpr([('c_custkey', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'c_custkey')), 
                                           ('c_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'c_name')), 
                                           ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'c_acctbal')), 
                                           ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'c_phone')), 
                                           ('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_name')), 
                                           ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'c_address')), 
                                           ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("x_na_cu_ord"), 0), 'c_comment'))]))]), 
                True), 
        LetExpr(VarExpr("na_part"), 
                SumExpr(VarExpr("x_na"), 
                        VarExpr("na"), 
                        DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_nationkey'), 
                                      RecConsExpr([('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_name'))]))]), 
                        True), 
                LetExpr(VarExpr("cu_part"), 
                        SumExpr(VarExpr("x_cu"), 
                                VarExpr("db->cu_dataset"), 
                                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_custkey'), 
                                              RecConsExpr([('c_name', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_name')), 
                                                           ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_acctbal')), 
                                                           ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_address')), 
                                                           ('c_nationkey', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_nationkey')), 
                                                           ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_phone')), 
                                                           ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_comment'))]))]), 
                                True), LetExpr(VarExpr("cu_ord"), 
                                               SumExpr(VarExpr("x_ord"), 
                                                       VarExpr("db->ord_dataset"), 
                                                       IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19940101))), 
                                                              IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), ConstantExpr(None)), 
                                                                     DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                                                                   RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_custkey')), 
                                                                                                ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_name')), 
                                                                                                ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_acctbal')), 
                                                                                                ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_address')), 
                                                                                                ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_phone')), 
                                                                                                ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_comment')), 
                                                                                                ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_part"), RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_nationkey')), 'n_name'))]))]), 
                                                                     EmptyDicConsExpr()), 
                                                              EmptyDicConsExpr()), 
                                                       False), LetExpr(VarExpr("na_cu_ord_li"), 
                                                                       SumExpr(VarExpr("x_li"), 
                                                                               VarExpr("db->li_dataset"), 
                                                                               IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_returnflag'), ConstantExpr("R")), 
                                                                                      IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                                                             DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_custkey')), 'c_custkey')), 
                                                                                                                        ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_name')), 'c_name')), 
                                                                                                                        ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_acctbal')), 'c_acctbal')), 
                                                                                                                        ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_phone')), 'c_phone')), 
                                                                                                                        ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'n_name')), 'n_name')), 
                                                                                                                        ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_address')), 'c_address')), 
                                                                                                                        ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_comment')), 'c_comment'))]), 
                                                                                                           RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                                                                             EmptyDicConsExpr()), 
                                                                                      EmptyDicConsExpr()), 
                                                                               False), LetExpr(VarExpr("result"), SumExpr(VarExpr("v8"), VarExpr("na_cu_ord_li"), DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v8"), 0), PairAccessExpr(VarExpr("v8"), 1)), ConstantExpr(True))]), True), 
                                                                                               LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True))))))))
```
```python
LetExpr(VarExpr("na_part"), 
        SumExpr(VarExpr("x_na"), 
                VarExpr("na"), 
                DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_nationkey'), 
                              RecConsExpr([('n_name', RecAccessExpr(PairAccessExpr(VarExpr("x_na"), 0), 'n_name'))]))]), True), 
        LetExpr(VarExpr("cu_part"), 
                SumExpr(VarExpr("x_cu"), 
                        VarExpr("db->cu_dataset"), 
                        DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_custkey'), 
                                      RecConsExpr([('c_name', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_name')),
                                                   ('c_acctbal', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_acctbal')), 
                                                   ('c_address', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_address')),
                                                   ('c_nationkey', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_nationkey')), 
                                                   ('c_phone', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_phone')), 
                                                   ('c_comment', RecAccessExpr(PairAccessExpr(VarExpr("x_cu"), 0), 'c_comment'))]))]), 
                        True), 
                LetExpr(VarExpr("cu_ord"), 
                        SumExpr(VarExpr("x_ord"), 
                                VarExpr("db->ord_dataset"), 
                                IfExpr(MulExpr(CompareExpr(CompareSymbol.GTE, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19931001)), CompareExpr(CompareSymbol.LT, RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderdate'), ConstantExpr(19940101))), 
                                       IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), ConstantExpr(None)), 
                                              DicConsExpr([(RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_orderkey'), 
                                                            RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_custkey')), 
                                                                         ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_name')), 
                                                                         ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_acctbal')), 
                                                                         ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_address')), 
                                                                         ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_phone')), 
                                                                         ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_comment')), 
                                                                         ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_part"), RecAccessExpr(DicLookupExpr(VarExpr("cu_part"), RecAccessExpr(PairAccessExpr(VarExpr("x_ord"), 0), 'o_custkey')), 'c_nationkey')), 'n_name'))]))]), 
                                              EmptyDicConsExpr()), 
                                       EmptyDicConsExpr()), 
                                False), 
                        LetExpr(VarExpr("na_cu_ord_li"), 
                                SumExpr(VarExpr("x_li"), 
                                        VarExpr("db->li_dataset"), 
                                        IfExpr(CompareExpr(CompareSymbol.EQ, RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_returnflag'), ConstantExpr("R")), 
                                               IfExpr(CompareExpr(CompareSymbol.NE, DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_orderkey')), ConstantExpr(None)), 
                                                      DicConsExpr([(RecConsExpr([('c_custkey', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_custkey')), 'c_custkey')), 
                                                                                 ('c_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_name')), 'c_name')), 
                                                                                 ('c_acctbal', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_acctbal')), 'c_acctbal')), 
                                                                                 ('c_phone', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_phone')), 'c_phone')), 
                                                                                 ('n_name', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'n_name')), 'n_name')), 
                                                                                 ('c_address', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_address')), 'c_address')), 
                                                                                 ('c_comment', RecAccessExpr(DicLookupExpr(VarExpr("na_cu_ord"), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'c_comment')), 'c_comment'))]), 
                                                                    RecConsExpr([('revenue', MulExpr(RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_extendedprice'), SubExpr(ConstantExpr(1), RecAccessExpr(PairAccessExpr(VarExpr("x_li"), 0), 'l_discount'))))]))]), 
                                                      EmptyDicConsExpr()), 
                                               EmptyDicConsExpr()), 
                                        False), 
                                LetExpr(VarExpr("result"), 
                                        SumExpr(VarExpr("v8"), 
                                                VarExpr("na_cu_ord_li"), 
                                                DicConsExpr([(ConcatExpr(PairAccessExpr(VarExpr("v8"), 0), PairAccessExpr(VarExpr("v8"), 1)), ConstantExpr(True))]), True), 
                                        LetExpr(VarExpr("out"), VarExpr("result"), ConstantExpr(True)))))))
```
`ispassby`
`part_frame.is_joint`
`probe_frame.is_joint`