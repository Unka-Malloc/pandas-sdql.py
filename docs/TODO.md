# Hyper-Opt-Python
TODO: Results, SF 0.1, text file

# Hyper-Unopt-Python

# Hyper-Opt-Compiler
- Q5: Fixed (Changing from `"1996-12-31"` to `"1995-01-01"`)
- Q9: Fixed (Changing from `"g"` to `"green"`)
- Q15: Fixed (Changing from `1614410.2928` to `1772627.2087`)
- Q17: 
  1. Changing from `('Brand#11', 'WRAP CASE')` to `("Brand#23", "MED BOX")`
  2. Carefully checked syntax and semantic of query
  3. Very Unexpected 0.0
  4. Tried: {`create a variable of scalar rather than a record with single value`: Failed, 
      `use a unique name for element in the record`: Failed,
      `use the original code provided in SDQL.py`: Failed
      }
  5. Mysterious Bug! 
    ```
    # This one failed!
    part_l1_lineitem = li.sum(lambda x_lineitem: record({"price": ((x_lineitem[0].l_extendedprice) if (
                x_lineitem[0].l_quantity < ((0.2) * (
        ((part_l1[x_lineitem[0].l_partkey].sum_quant) / (part_l1[x_lineitem[0].l_partkey].count_quant))))) else (
        0.0)) if (part_l1[x_lineitem[0].l_partkey] != None) else (0.0)}))
    ```
  
    ```
    # This one passed, with a meaningless (if (True) else (None)) at the end!
    part_l1_lineitem = li.sum(lambda x_lineitem: record({"price": ((x_lineitem[0].l_extendedprice) if (
                x_lineitem[0].l_quantity < ((0.2) * (
        ((part_l1[x_lineitem[0].l_partkey].sum_quant) / (part_l1[x_lineitem[0].l_partkey].count_quant))))) else (
        0.0)) if (part_l1[x_lineitem[0].l_partkey] != None) else (0.0)}) if (True) else (None))
    ```

# Hyper-Unopt-Compiler
- Fixed: Q6, Q19, Q22 from iteration on a record