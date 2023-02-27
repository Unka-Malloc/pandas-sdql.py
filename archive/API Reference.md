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

# Plan
2, 5, 7, 8, 9, 11, 12, 13, 17

# Possible Operations as input()
1. Agg: Aggregation
2. GroupbyAgg: Group-By Aggregation
3. JoinPartition: Partition Side (Hash Join)
4. JoinProbe: Probe Side (Hash Join)
5. Joint: Joint DataFrame (Hash Join)
6. Calc: Addition, Multiplication, Subtraction, and Division
7. ColProj: Column Projection

# Possible Branches
1. this (joint) is the last joint frame and has last operation: groupby aggregation
2. this (joint) is the last joint frame and has last operation: aggregation
3. this (joint) is the last joint frame and has last operation: calculation
4. this (joint) is the next probe side
5. this (joint) is the next partition side
6. probe side has last operation groupby aggregation
7. probe side has operation isin()

```
if self.is_last_joint:
    if self.last_iter_is_groupby_aggr:
        if probe.is_joint:
            if part.as_bypass_for_next_join:
                # partition side is completely bypassed
                Q5
            else:
                # partition side contains some columns that are used in the future
                # probe on each partition side
                Q7
        else:
            # probe_side is not joint
            # what about part side?
            # 1. part side was probed -> possible multiple partitions
            #   - part side is joint -> part side + 1
            #   - part side has isin -> part side + 1
            # 2. part side was not probed -> at most 2 partitions
            #   - 1st part (neccessary): from merge
            #   - 2nd part (possible): from isin 
    
    if self.last_iter_is_calc:
        Q14
    
    if self.last_iter_is_merge:
    
    if self.probe_side_was_groupby_agg:
    
    if self.probe_side_was_isin:
    
    if self.part_side_as_bypass_for_next_join:
    
else:
    if self.as_part_for_next_join:
    
    if self.as_probe_for_next_join:
    
    if self.as_bypass_for_next_join:
        Q5
        '''
        When: 
            1. This dataframe is merged as the partition side. (left=this)
            2. The columns of this dataframe is never used in the operations after the merge.
        Then:
            1. Extract columns from this merge as partition keys (left_on=[col1, col2, ...])
            2. Check these columns are not None with probe keys (right_on=[col1, col2, ...])
        '''
```

```python
groupby_cols = []
aggr_dict = {}

# aggr = {? : scalar}
if len(aggr_dict.keys()) == 1:
    # aggr = {scalar : scalar}
    if len(groupby_cols) == 1:
        pass
    # aggr = {record : scalar}
    else:
        pass
# aggr = {? : record}
else:
    # aggr = {scalar: record}
    if len(groupby_cols) == 1:
        pass
    # aggr = {record : record}
    else:
        pass
```

# GroupbyAggrExpr and AggrExpr
GroupbyAggrExpr
- groupby_cols: `List[str]`
- origin_dict: `{new_col: (old_col, func)}`

AggrExpr
- origin_dict: `{new_col: (old_col, func)}`

# FlexIR
- DataFrame 
- IterEl
- ColEl
- ColOpExpr
- ColExtExpr
- AggrExpr
- GroupbyAggrExpr
- VarBindExpr
- VarBindExpr
