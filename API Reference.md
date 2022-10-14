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
