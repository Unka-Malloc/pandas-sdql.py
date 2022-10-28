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