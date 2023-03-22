from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    brand12 = "Brand#12"
    smcase = "SM CASE"
    smbox = "SM BOX"
    smpack = "SM PACK"
    smpkg = "SM PKG"
    brand23 = "Brand#23"
    medbag = "MED BAG"
    medbox = "MED BOX"
    medpkg = "MED PKG"
    medpack = "MED PACK"
    brand34 = "Brand#34"
    lgcase = "LG CASE"
    lgbox = "LG BOX"
    lgpack = "LG PACK"
    lgpkg = "LG PKG"
    air = "AIR"
    airreg = "AIR REG"
    deliverinperson = "DELIVER IN PERSON"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].l_shipmode == air) + (x[0].l_shipmode == airreg))) * (x[0].l_shipinstruct == deliverinperson))) else (None))
    
    part_lineitem_probe = v0
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((((((((((x[0].p_brand == brand12) * (((((((x[0].p_container == smpkg) + (x[0].p_container == smpack))) + (x[0].p_container == smcase))) + (x[0].p_container == smbox))))) * (x[0].p_size >= 1))) * (x[0].p_size <= 5))) + (((((((x[0].p_brand == brand23) * (((((((x[0].p_container == medpack) + (x[0].p_container == medpkg))) + (x[0].p_container == medbag))) + (x[0].p_container == medbox))))) * (x[0].p_size >= 1))) * (x[0].p_size <= 10))))) + (((((((x[0].p_brand == brand34) * (((((((x[0].p_container == lgpkg) + (x[0].p_container == lgpack))) + (x[0].p_container == lgcase))) + (x[0].p_container == lgbox))))) * (x[0].p_size >= 1))) * (x[0].p_size <= 15))))) else (None))
    
    part_lineitem_part = v0
    build_side = part_lineitem_part.sum(lambda x: ({x[0].p_partkey: sr_dict({x[0]: x[1]})}) if (True) else (None))
    
    v0 = part_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_partkey] != None) else (None))
    
    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (((((((x[0].p_brand == brand12) * (((x[0].l_quantity >= 1) * (x[0].l_quantity <= 11))))) + (((x[0].p_brand == brand23) * (((x[0].l_quantity >= 10) * (x[0].l_quantity <= 20))))))) + (((x[0].p_brand == brand34) * (((x[0].l_quantity >= 20) * (x[0].l_quantity <= 30))))))) else (None))
    
    v2 = v1.sum(lambda x: ({x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (True) else (None))
    
    v3 = v2.sum(lambda x: (record({"revenue": x[0].revenue})) if (True) else (None))
    
    v4 = v3.sum(lambda x: ({v3: True}) if (True) else (None))
    
    results = v4
    # Complete

    return results
