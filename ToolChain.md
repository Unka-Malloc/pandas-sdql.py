## SDQL IR List
`[LetExpr,
SumExpr,
IfExpr,
CompareExpr, 
MulExpr, AddExpr, DivExpr, SubExpr,
DicConsExpr, DicLookupExpr,
RecConsExpr, RecAccessExpr, PairAccessExpr,
ConcatExpr,
ExtFuncExpr
]`

## pysdql tool chain
`concat_cond`: Concatenate the new condition to the original condition with `and` \ `or`  
`add_cond`: Add the new condition to the outer or inner layer of the original condition  
`replace_cond`: Replace the old condition with a new condition  

`replace_dict`: Find all occurrences of the specified dictionary and replace it with a new one.  
`replace_rec`: Find all occurrences of the specified record and replace it with a new one.  
`replace_iter_el`: Find all occurrences of the specified iteration element and replace it with a new one.  

`get_ret_type`: Get the type of return value after the iteration.  
`is_ret_dict`: This iteration returns a dictionary  
`is_ret_rec` This iteration returns a record  
`is_ret_val` This iteration returns a value  

`is_key_rec`: This iteration takes a record as the dict key.  
`is_val_rec`: This iteration takes a record as the dict value.  
`is_key_val`: This iteration takes a value as the dict key.  
`is_val_rec`: This iteration takes a value as the dict value.

`replace_op`: Replace the iteration operation with a new operation  
`replace_op_dict_key`: If the iteration operation is a dict, then replace the key with a new one.  
`replace_op_dict_val`: If the iteration operation is a dict, then replace the value with a new one.

`findall_var`: Find all variables  
`findall_const`: Find all constants  
`define_var`: Define all variables  
`define_const`: Define all constants  
