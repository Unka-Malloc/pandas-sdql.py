# All Queries
1. iteration elements: separately defined
2. iteration dictionary: separately defined
3. var out: separately defined 
   out = VarExpr('out')
   LetExpr(out, ..., ConstantExpr(True))
4. single output: query
   change q1... to query
5. isUpdate: Calculation required
6. isAssignment: Unique Key / No calculation
7. {<col = ...> -> ...} ? {col -> ...} ()
8. {k -> <col = ?>} ? {k -> col} (Q4: order_count)

# Order?
conflict name `orders` with `ord`
 
# Q6
out = VarExpr('out')
query = LetExpr(out, SumExpr(...), ConstantExpr(True))

out is directly binded with SumExpr

# Q1
Nothing

# Q3
Unused variables?

# Q19
Partition with isAssignment = True?
There is a condition after the merge.

# Q4 
There is a dense array, but never occur in generated code?

# Q15
max_revenue -> 