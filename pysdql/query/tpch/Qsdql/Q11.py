from pysdql.query.tpch.const import (PARTSUPP_TYPE, SUPPLIER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ps": PARTSUPP_TYPE, "su": SUPPLIER_TYPE, "na": NATION_TYPE})
def query(ps, su, na):

    # Insert
    germany = "GERMANY"
    nation_supplier_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))
    
    nation_supplier_build_nest_dict = nation_supplier_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_partsupp_build_pre_ops = su.sum(lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))
    
    nation_supplier_partsupp_build_nest_dict = nation_supplier_partsupp_build_pre_ops.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})
    
    nation_supplier_partsupp_0 = ps.sum(lambda x: (nation_supplier_partsupp_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (nation_supplier_partsupp_build_nest_dict[x[0].ps_suppkey] != None) else (None))
    
    tmp_var_ps_supplycost_mul_ps_availqty_mul_00001 = nation_supplier_partsupp_0.sum(lambda x: ((((x[0].ps_supplycost) * (x[0].ps_availqty))) * (0.0001)))
    
    nation_supplier_partsupp_1 = nation_supplier_partsupp_0.sum(lambda x: ({x[0]: x[1]}) if (tmp_var_ps_supplycost_mul_ps_availqty_mul_00001 < ((x[0].ps_supplycost) * (x[0].ps_availqty))) else (None))
    
    nation_supplier_partsupp_2 = nation_supplier_partsupp_1.sum(lambda x: {x[0].concat(record({"value": ((x[0].ps_supplycost) * (x[0].ps_availqty))})): x[1]})
    
    nation_supplier_partsupp_3 = nation_supplier_partsupp_2.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"value": x[0].value})})
    
    results = nation_supplier_partsupp_3.sum(lambda x: {x[0].concat(x[1]): True})
    
    # Complete

    return results
