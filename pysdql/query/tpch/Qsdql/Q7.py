from pysdql.query.tpch.const import (SUPPLIER_TYPE, LINEITEM_TYPE, ORDERS_TYPE, CUSTOMER_TYPE, NATION_TYPE)

from pysdql.extlib.sdqlpy.sdql_lib import *

@sdql_compile({"su": SUPPLIER_TYPE, "li": LINEITEM_TYPE, "ord": ORDERS_TYPE, "cu": CUSTOMER_TYPE, "na": NATION_TYPE})
def query(su, li, ord, cu, na):

    # Insert
    france = "FRANCE"
    germany = "GERMANY"
    v0 = li.sum(lambda x: (({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None)) if (x[0] != None) else (None))
    
    nation_customer_orders_lineitem_probe = v0
    nation_customer_orders_probe = ord
    nation_customer_probe = cu
    v0 = na.sum(lambda x: (({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_part = v0
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None)) if (x[0] != None) else (None))
    
    nation_customer_part = v1
    build_side = nation_customer_part.sum(lambda x: (({x[0].n_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_customer_probe.sum(lambda x: (({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].c_nationkey] != None) else (None)) if (x[0] != None) else (None))
    
    nation_customer_orders_part = v0
    build_side = nation_customer_orders_part.sum(lambda x: (({x[0].c_custkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_customer_orders_probe.sum(lambda x: (({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].o_custkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0].concat(record({"n2_name": x[0].n_name})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    nation_customer_orders_lineitem_part = v1
    build_side = nation_customer_orders_lineitem_part.sum(lambda x: (({x[0].o_orderkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_customer_orders_lineitem_probe.sum(lambda x: (({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_orderkey] != None) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_nation_customer_orders_lineitem_probe = v0
    nation_supplier_probe = su
    v0 = na.sum(lambda x: (({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: (({x[0].n_nationkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_supplier_probe.sum(lambda x: (({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].s_nationkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0].concat(record({"n1_name": x[0].n_name})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    nation_supplier_nation_customer_orders_lineitem_part = v1
    build_side = nation_supplier_nation_customer_orders_lineitem_part.sum(lambda x: (({x[0].s_suppkey: sr_dict({x[0]: x[1]})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v0 = nation_supplier_nation_customer_orders_lineitem_probe.sum(lambda x: (({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
    : True}) if (build_side[x[0].l_suppkey] != None) else (None)) if (x[0] != None) else (None))
    
    v1 = v0.sum(lambda x: (({x[0]: x[1]}) if (((((x[0].n1_name == france) * (x[0].n2_name == germany))) + (((x[0].n1_name == germany) * (x[0].n2_name == france))))) else (None)) if (x[0] != None) else (None))
    
    v2 = v1.sum(lambda x: (({x[0].concat(record({"supp_nation": x[0].n1_name})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v3 = v2.sum(lambda x: (({x[0].concat(record({"cust_nation": x[0].n2_name})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v4 = v3.sum(lambda x: (({x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v5 = v4.sum(lambda x: (({x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v6 = v5.sum(lambda x: (({x[0]: x[1]}) if (True) else (None)) if (x[0] != None) else (None))
    
    v7 = v6.sum(lambda x: (({record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record({"revenue": x[0].volume})}) if (True) else (None)) if (x[0] != None) else (None))
    
    v8 = v7.sum(lambda x: (({x[0].concat(x[1]): True}) if (True) else (None)) if (x[0] != None) else (None))
    
    results = v8
    # Complete

    return results
