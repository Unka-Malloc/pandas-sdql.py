from pysdql.query.tpch.const import (PARTSUPP_TYPE, SUPPLIER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ps": PARTSUPP_TYPE, "su": SUPPLIER_TYPE, "na": NATION_TYPE})
def query(ps, su, na):

    # Insert
    germany = "GERMANY"
    nation_supplier_partsupp_probe = ps
    nation_supplier_probe = su
    v0 = na.sum(lambda x: (({x[0]: x[1]}) if (x[0].n_name == germany) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: (({x[0].n_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_supplier_probe.sum(lambda x: (({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_partsupp_part = v0
    build_side = nation_supplier_partsupp_part.sum(lambda x: (({x[0].s_suppkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_supplier_partsupp_probe.sum(lambda x: (({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].ps_suppkey] != None) else (None)) if (x[0] != None) else (None))
    
    tmp_calc_value = v0.sum(lambda x: ((((((x[0].ps_supplycost) * (x[0].ps_availqty))) * (0.0001))) if (True) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (tmp_calc_value < ((x[0].ps_supplycost) * (x[0].ps_availqty))) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({x[0].concat(record({"value": ((x[0].ps_supplycost) * (x[0].ps_availqty))})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({record({"ps_partkey": x[0].ps_partkey}): record({"value": x[0].value})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v4 = v3.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v4
    # Complete

    return results
