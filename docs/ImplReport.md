# Python Mode
## Error
### General
#### Failed to create singleton dictionary
This is a general error, and can occur at any position in any query.

When creating a singleton dictionary by constructing a record, it sometimes ignores the dictionary construction and returns the result of the previous step directly.

### Specific Query
#### Q13: Not following the TPC-H standard query, our query changes the conditions, even in SDQL.py.

# Appendix

# PostgreSQL
## Difference
- Q13: External Function `LastIndex` required!
- Q15: Replace `max()` with `float`  
    | magic number: 1614410.2928000002 sometimes works (unstable, unpredictable)


## Unopt
- Q22: ColExtExpr has no attribute isin
- Q21: 'ColEl' object has no attribute 'create_copy'

[comment]: <> (- Q15: 'NoneType' object has no attribute 'descriptor')

[comment]: <> (- Q11: NotImplemented > NewColListExpr)

- Q9: NoneType not callable
- Q8: Not Implemented > apply()
- Q7: Max Recursion

## First Round
- Q8
- Q9
- Q11
- Q15

## Last Round
- Q7: outer, \_x and \_y
- Q21: outer, \_x and \_y
- Q22: outer