from pysdql.query.tpch.const import (PARTSUPP_TYPE, SUPPLIER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"ps": PARTSUPP_TYPE, "su": SUPPLIER_TYPE, "na": NATION_TYPE})
def query(ps, su, na):

    # Insert
    germany = "GERMANY"
    nation_part = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: True}) if (x_nation[0].n_name == germany) else (None))
    
    nation_supplier = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: True}) if (nation_part[x_supplier[0].s_nationkey] != None) else (None))
    
    partsupp_aggr = ps.sum(lambda x_partsupp: (record({"filt_val": ((((x_partsupp[0].ps_supplycost) * (x_partsupp[0].ps_availqty))) * (0.0001)), "filt_agg": sr_dict({x_partsupp[0].ps_partkey: ((x_partsupp[0].ps_supplycost) * (x_partsupp[0].ps_availqty))})})) if (nation_supplier[x_partsupp[0].ps_suppkey] != None) else (None))
    
    results = partsupp_aggr.filt_agg.sum(lambda x_partsupp_aggr: ({record({"ps_partkey": x_partsupp_aggr[0], "value": x_partsupp_aggr[1]}): True}) if (x_partsupp_aggr[1] > partsupp_aggr.filt_val) else (None))
    
    # Complete

    return results
