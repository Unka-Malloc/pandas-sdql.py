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
 
# Q6
out = VarExpr('out')
query = LetExpr(out, SumExpr(...), ConstantExpr(True))

out is directly binded with SumExpr

# Q1