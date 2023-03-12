from pysdql.query.tpch.const import (LINEITEM_TYPE, PART_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"li": LINEITEM_TYPE, "pa": PART_TYPE})
def query(li, pa):

    # Insert
    air = "AIR"
    airreg = "AIR REG"
    deliverinperson = "DELIVER IN PERSON"
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
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_partkey: record({"l_orderkey": x_lineitem[0].l_orderkey, "l_partkey": x_lineitem[0].l_partkey, "l_suppkey": x_lineitem[0].l_suppkey, "l_linenumber": x_lineitem[0].l_linenumber, "l_quantity": x_lineitem[0].l_quantity, "l_extendedprice": x_lineitem[0].l_extendedprice, "l_discount": x_lineitem[0].l_discount, "l_tax": x_lineitem[0].l_tax, "l_returnflag": x_lineitem[0].l_returnflag, "l_linestatus": x_lineitem[0].l_linestatus, "l_shipdate": x_lineitem[0].l_shipdate, "l_commitdate": x_lineitem[0].l_commitdate, "l_receiptdate": x_lineitem[0].l_receiptdate, "l_shipinstruct": x_lineitem[0].l_shipinstruct, "l_shipmode": x_lineitem[0].l_shipmode, "l_comment": x_lineitem[0].l_comment})}) if (((((((x_lineitem[0].l_shipmode == airreg) + (x_lineitem[0].l_shipmode == air))) * (x_lineitem[0].l_shipinstruct == deliverinperson))) * (((((((x_lineitem[0].l_quantity >= 1) * (x_lineitem[0].l_quantity <= 11))) + (((x_lineitem[0].l_quantity >= 10) * (x_lineitem[0].l_quantity <= 20))))) + (((x_lineitem[0].l_quantity >= 20) * (x_lineitem[0].l_quantity <= 30))))))) else (None))
    
    part_aggr = pa.sum(lambda x_part: (((((lineitem_part[x_part[0].p_partkey].l_extendedprice) * (((1) - (lineitem_part[x_part[0].p_partkey].l_discount))))) if (((((((((((((x_part[0].p_brand == brand12) * (((((((x_part[0].p_container == smpkg) + (x_part[0].p_container == smpack))) + (x_part[0].p_container == smcase))) + (x_part[0].p_container == smbox))))) * (lineitem_part[x_part[0].p_partkey].l_quantity >= 1))) * (lineitem_part[x_part[0].p_partkey].l_quantity <= 11))) * (x_part[0].p_size <= 5))) + (((((((((x_part[0].p_brand == brand23) * (((((((x_part[0].p_container == medpack) + (x_part[0].p_container == medpkg))) + (x_part[0].p_container == medbag))) + (x_part[0].p_container == medbox))))) * (lineitem_part[x_part[0].p_partkey].l_quantity >= 10))) * (lineitem_part[x_part[0].p_partkey].l_quantity <= 20))) * (x_part[0].p_size <= 10))))) + (((((((((x_part[0].p_brand == brand34) * (((((((x_part[0].p_container == lgpkg) + (x_part[0].p_container == lgpack))) + (x_part[0].p_container == lgcase))) + (x_part[0].p_container == lgbox))))) * (lineitem_part[x_part[0].p_partkey].l_quantity >= 20))) * (lineitem_part[x_part[0].p_partkey].l_quantity <= 30))) * (x_part[0].p_size <= 15))))) else (0)) if (lineitem_part[x_part[0].p_partkey] != None) else (0)) if (((x_part[0].p_size >= 1) * (((((((((x_part[0].p_brand == brand12) * (((((((x_part[0].p_container == smpkg) + (x_part[0].p_container == smpack))) + (x_part[0].p_container == smcase))) + (x_part[0].p_container == smbox))))) * (x_part[0].p_size <= 5))) + (((((x_part[0].p_brand == brand23) * (((((((x_part[0].p_container == medpack) + (x_part[0].p_container == medpkg))) + (x_part[0].p_container == medbag))) + (x_part[0].p_container == medbox))))) * (x_part[0].p_size <= 10))))) + (((((x_part[0].p_brand == brand34) * (((((((x_part[0].p_container == lgpkg) + (x_part[0].p_container == lgpack))) + (x_part[0].p_container == lgcase))) + (x_part[0].p_container == lgbox))))) * (x_part[0].p_size <= 15))))))) else (0))
    
    results = {record({"revenue": part_aggr}): True}
    # Complete

    return results
