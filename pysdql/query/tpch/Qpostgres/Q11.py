from pysdql.query.tpch.const import (PARTSUPP_TYPE, SUPPLIER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ps": PARTSUPP_TYPE, "su": SUPPLIER_TYPE, "na": NATION_TYPE})
def query(ps, su, na):

    # Insert
    germany = "GERMANY"
    supplier_nation_probe = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))
    
    nation_1 = nation_0.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))
    
    supplier_nation_probe = nation_2.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))
    
    supplier_nation_build_nest_dict = supplier_nation_index.sum(lambda x: {x[0].s_nationkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_probe = supplier_nation_probe.sum(lambda x: (supplier_nation_build_nest_dict[x[0].n_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (supplier_nation_build_nest_dict[x[0].n_nationkey] != None) else (None))
    
    partsupp_supplier_nation_build_nest_dict = partsupp_supplier_nation_index.sum(lambda x: {x[0].ps_suppkey: sr_dict({x[0]: x[1]})})
    
    partsupp_supplier_nation_0 = partsupp_supplier_nation_probe.sum(lambda x: (partsupp_supplier_nation_build_nest_dict[x[0].s_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
    ) if (partsupp_supplier_nation_build_nest_dict[x[0].s_suppkey] != None) else (None))
    
    partsupp_supplier_nation_1 = partsupp_supplier_nation_0.sum(lambda x: {x[0].concat(record({"before_1": ((x[0].ps_supplycost) * (x[0].ps_availqty))})): x[1]})
    
    partsupp_supplier_nation_2 = partsupp_supplier_nation_1.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"value": x[0].before_1, "sum_before_1": x[0].before_1})})
    
    partsupp_supplier_nation_3 = partsupp_supplier_nation_2.sum(lambda x: {x[0].concat(x[1]): True})
    
    partsupp_supplier_nation_4 = partsupp_supplier_nation_3.sum(lambda x: {x[0].concat(record({"sumps_supplycostps_availqty": x[0].sum_before_1})): x[1]})
    
    results = partsupp_supplier_nation_4.sum(lambda x: ({x[0]: x[1]}) if (x[0].sumps_supplycostps_availqty > ((((x_partsupp[0].ps_supplycost) * (x_partsupp[0].ps_availqty))) * (0.0001))) else (None))
    
    # Complete

    return results
