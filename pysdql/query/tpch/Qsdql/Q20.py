from pysdql.query.tpch.const import (SUPPLIER_TYPE, NATION_TYPE, PARTSUPP_TYPE, PART_TYPE, LINEITEM_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *


@sdql_compile({"su": SUPPLIER_TYPE, "na": NATION_TYPE, "ps": PARTSUPP_TYPE, "pa": PART_TYPE, "li": LINEITEM_TYPE})
def query(su, na, ps, pa, li):

    # Insert
    canada = "CANADA"
    forest = "forest"
    isin_build = su.sum(lambda x: (({x[0].s_suppkey: True}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = li.sum(lambda x: ((({x[0]: True}) if (isin_build[x[0].l_suppkey] != None) else (None)) if (True) else (None)) if (x[0] != None) else (None))
    
    isin_build = v0.sum(lambda x: (({x[0].l_suppkey: True}) if (True) else (None)) if (x[0] != None) else (None))
    
    v1 = su.sum(lambda x: ((({x[0]: True}) if (isin_build[x[0].s_suppkey] != None) else (None)) if (True) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_probe = v1
    v0 = na.sum(lambda x: (({x[0]: x[1]}) if (x[0].n_name == canada) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: (({x[0].n_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_supplier_probe.sum(lambda x: (({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v1
    # Complete

    return results
