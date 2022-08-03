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