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
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: record({"p_partkey": x_part[0].p_partkey, "p_brand": x_part[0].p_brand})}) if (((((((((((x_part[0].p_brand == brand12) * (((((((x_part[0].p_container == smpkg) + (x_part[0].p_container == smpack))) + (x_part[0].p_container == smcase))) + (x_part[0].p_container == smbox))))) * (x_part[0].p_size >= 1))) * (x_part[0].p_size <= 5))) + (((((((x_part[0].p_brand == brand23) * (((((((x_part[0].p_container == medpack) + (x_part[0].p_container == medpkg))) + (x_part[0].p_container == medbag))) + (x_part[0].p_container == medbox))))) * (x_part[0].p_size >= 1))) * (x_part[0].p_size <= 10))))) + (((((((x_part[0].p_brand == brand34) * (((((((x_part[0].p_container == lgpkg) + (x_part[0].p_container == lgpack))) + (x_part[0].p_container == lgcase))) + (x_part[0].p_container == lgbox))))) * (x_part[0].p_size >= 1))) * (x_part[0].p_size <= 15))))) else (None))
    
    lineitem_aggr = li.sum(lambda x_lineitem: (((((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) if (((((((part_part[x_lineitem[0].l_partkey].p_brand == brand12) * (((x_lineitem[0].l_quantity >= 1) * (x_lineitem[0].l_quantity <= 11))))) + (((part_part[x_lineitem[0].l_partkey].p_brand == brand23) * (((x_lineitem[0].l_quantity >= 10) * (x_lineitem[0].l_quantity <= 20))))))) + (((part_part[x_lineitem[0].l_partkey].p_brand == brand34) * (((x_lineitem[0].l_quantity >= 20) * (x_lineitem[0].l_quantity <= 30))))))) else (0.0)) if (part_part[x_lineitem[0].l_partkey] != None) else (0.0)) if (((((x_lineitem[0].l_shipmode == air) + (x_lineitem[0].l_shipmode == airreg))) * (x_lineitem[0].l_shipinstruct == deliverinperson))) else (0.0))
    
    results = {record({"revenue": lineitem_aggr}): True}
    # Complete

    return results
