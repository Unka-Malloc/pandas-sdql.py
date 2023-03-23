import os
from sdqlpy.sdql_lib import *

## The "sdqlpy_init" function must be defined here.
## Its first parameter can be set to:
## 0: to run in Python | 1: to run in compiled mode | 2: to run in compiled mode using the previously compiled version.
## Its second parameter show the number of threads. (current version does not support multi-threading for "run in Python" mode)
## Note: when the first parameter is set to "1", the previous number of threads is used and the current parameter will be ignored.

sdqlpy_init(1, 4)

############ Reading Dataset

## The following path must point to your dbgen dataset.
dataset_path = os.getenv('TPCH_DATASET')

## Shows the number of returned results, average and stdev of run time, and the results (if the next parameter is also set to True)
verbose = True
show_results = False

## Number of iterations for benchmarking each query (must be >=2)
iterations = 10

############ Reading Dataset

lineitem_type = {record(
    {"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float,
     "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1),
     "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date,
     "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
customer_type = {record(
    {"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15),
     "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
order_type = {record(
    {"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date,
     "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79),
     "o_NA": string(1)}): bool}
nation_type = {record(
    {"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152), "n_NA": string(1)}): bool}
region_type = {record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}
part_type = {record(
    {"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25),
     "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23),
     "p_NA": string(1)}): bool}
partsupp_type = {record(
    {"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199),
     "ps_NA": string(1)}): bool}
supplier_type = {record(
    {"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15),
     "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}

lineitem = read_csv(dataset_path + "lineitem.tbl", lineitem_type, "li")
customer = read_csv(dataset_path + "customer.tbl", customer_type, "cu")
order = read_csv(dataset_path + "orders.tbl", order_type, "ord")
nation = read_csv(dataset_path + "nation.tbl", nation_type, "na")
region = read_csv(dataset_path + "region.tbl", region_type, "re")
part = read_csv(dataset_path + "part.tbl", part_type, "pa")
partsupp = read_csv(dataset_path + "partsupp.tbl", partsupp_type, "ps")
supplier = read_csv(dataset_path + "supplier.tbl", supplier_type, "su")


######

@sdql_compile({"li": lineitem_type})
def q1(li):
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate <= 19980902) else (None))

    v1 = v0.sum(
        lambda x: {x[0].concat(record({"disc_price": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v2 = v1.sum(lambda x: {x[0].concat(
        record({"charge": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) * (((1.0) + (x[0].l_tax))))})): x[
        1]})

    v3 = v2.sum(lambda x: {record({"l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus}): record(
        {"sum_qty": x[0].l_quantity, "sum_base_price": x[0].l_extendedprice, "sum_disc_price": x[0].disc_price,
         "sum_charge": x[0].charge, "avg_qty_sum_for_mean": x[0].l_quantity, "avg_qty_count_for_mean": 1.0,
         "avg_price_sum_for_mean": x[0].l_extendedprice, "avg_price_count_for_mean": 1.0,
         "avg_disc_sum_for_mean": x[0].l_discount, "avg_disc_count_for_mean": 1.0, "count_order": 1.0})})

    v4 = v3.sum(lambda x: {record(
        {"l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "sum_qty": x[1].sum_qty,
         "sum_base_price": x[1].sum_base_price, "sum_disc_price": x[1].sum_disc_price, "sum_charge": x[1].sum_charge,
         "avg_qty": ((x[1].avg_qty_sum_for_mean) / (x[1].avg_qty_count_for_mean)),
         "avg_price": ((x[1].avg_price_sum_for_mean) / (x[1].avg_price_count_for_mean)),
         "avg_disc": ((x[1].avg_disc_sum_for_mean) / (x[1].avg_disc_count_for_mean)),
         "count_order": x[1].count_order}): True})

    results = v4

    return results


######

@sdql_compile({"pa": part_type, "su": supplier_type, "ps": partsupp_type, "na": nation_type, "re": region_type})
def q2(pa, su, ps, na, re):
    europe = "EUROPE"
    brass = "BRASS"
    part_partsupp_probe = ps
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))

    part_partsupp_part = v0
    build_side = part_partsupp_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].ps_partkey] != None) else (None))

    region_nation_supplier_ps1_part_partsupp_probe = v0
    region_nation_supplier_ps1_probe = ps
    region_nation_supplier_probe = su
    region_nation_probe = na
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))

    region_nation_part = v0
    build_side = region_nation_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_probe.sum(lambda x: ({build_side[x[0].n_regionkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].n_regionkey] != None) else (None))

    region_nation_supplier_part = v0
    build_side = region_nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                                      : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    region_nation_supplier_ps1_part = v0
    build_side = region_nation_supplier_ps1_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_supplier_ps1_probe.sum(lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
                                                          : True}) if (build_side[x[0].ps_suppkey] != None) else (None))

    v1 = v0.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"ps_supplycost": x[0].ps_supplycost})})

    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})

    v3 = v2.sum(lambda x: {x[0].concat(record({"min_supplycost": x[0].ps_supplycost})): x[1]})

    region_nation_supplier_ps1_part_partsupp_part = v3
    build_side = region_nation_supplier_ps1_part_partsupp_part.sum(lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_supplier_ps1_part_partsupp_probe.sum(
        lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].ps_partkey] != None) else (None))

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe = v0
    region_nation_supplier_probe = su
    region_nation_probe = na
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))

    region_nation_part = v0
    build_side = region_nation_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_probe.sum(lambda x: ({build_side[x[0].n_regionkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].n_regionkey] != None) else (None))

    region_nation_supplier_part = v0
    build_side = region_nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                                      : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    region_nation_supplier_ps1_part = v0
    build_side = region_nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v1 = region_nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                                      : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_part = v1
    build_side = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_part.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe.sum(
        lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].ps_suppkey] != None) else (None))

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_supplycost == x[0].min_supplycost) else (None))

    results = v1

    return results


######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q3(li, cu, ord):
    building = "BUILDING"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate > 19950315) else (None))

    customer_orders_lineitem_probe = v0
    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderdate < 19950315) else (None))

    customer_orders_probe = v0
    v0 = cu.sum(lambda x: ({x[0]: x[1]}) if (x[0].c_mktsegment == building) else (None))

    customer_orders_part = v0
    build_side = customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    v0 = customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].o_custkey] != None) else (None))

    customer_orders_lineitem_part = v0
    build_side = customer_orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = customer_orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                                                        : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    v1 = v0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v2 = v1.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate,
                                   "o_shippriority": x[0].o_shippriority}): record({"revenue": x[0].revenue})})

    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})

    results = v3

    return results


#####

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q4(ord, li):
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_commitdate < x[0].l_receiptdate) else (None))

    orders_lineitem_isin_build = v0
    orders_lineitem_isin_build = orders_lineitem_isin_build.sum(lambda x: {x[0].l_orderkey: True})

    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (orders_lineitem_isin_build[x[0].o_orderkey] != None) else (None))

    v1 = v0.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None))

    v2 = v1.sum(lambda x: {record({"o_orderpriority": x[0].o_orderpriority}): record({"order_count": 1.0})})

    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})

    results = v3

    return results


#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type, "re": region_type, "na": nation_type,
               "su": supplier_type})
def q5(li, cu, ord, re, na, su):
    asia = "ASIA"
    region_nation_customer_orders_lineitem_probe = li
    v0 = ord.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19940101) * (x[0].o_orderdate < 19961231))) else (None))

    region_nation_customer_orders_probe = v0
    region_nation_customer_probe = cu
    region_nation_probe = na
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == asia) else (None))

    region_nation_part = v0
    build_side = region_nation_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_probe.sum(lambda x: ({build_side[x[0].n_regionkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].n_regionkey] != None) else (None))

    region_nation_customer_part = v0
    build_side = region_nation_customer_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_customer_probe.sum(lambda x: ({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
                                                      : True}) if (build_side[x[0].c_nationkey] != None) else (None))

    region_nation_customer_orders_part = v0
    build_side = region_nation_customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
                                                             : True}) if (build_side[x[0].o_custkey] != None) else (
        None))

    region_nation_customer_orders_lineitem_part = v0
    build_side = region_nation_customer_orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = region_nation_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    supplier_region_nation_customer_orders_lineitem_probe = v0
    supplier_region_nation_customer_orders_lineitem_part = su
    build_side = supplier_region_nation_customer_orders_lineitem_part.sum(
        lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_nationkey": x[0].s_nationkey}): sr_dict({x[0]: x[1]})})

    v0 = supplier_region_nation_customer_orders_lineitem_probe.sum(lambda x: (
    {build_side[record({"l_suppkey": x[0].l_suppkey, "c_nationkey": x[0].c_nationkey})].sum(lambda y: x[0].concat(y[0]))
     : True}) if (build_side[record({"l_suppkey": x[0].l_suppkey, "c_nationkey": x[0].c_nationkey})] != None) else (
        None))

    v1 = v0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v2 = v1.sum(lambda x: {record({"n_name": x[0].n_name}): record({"revenue": x[0].revenue})})

    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})

    results = v3

    return results


#######

@sdql_compile({"li": lineitem_type})
def q6(li):
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if ((((
                (((((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) * (x[0].l_discount >= 0.05))) * (
                    x[0].l_discount <= 0.07))) * (x[0].l_quantity < 24))) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (x[0].l_discount))})): x[1]})

    v2 = v1.sum(lambda x: record({"revenue": x[0].revenue}))

    v3 = v2.sum(lambda x: {v2: True})

    results = v3

    return results


#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type, "na": nation_type})
def q7(su, li, ord, cu, na):
    france = "FRANCE"
    germany = "GERMANY"
    v0 = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None))

    nation_customer_orders_lineitem_probe = v0
    nation_customer_orders_probe = ord
    nation_customer_probe = cu
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))

    nation_supplier_part = v0
    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))

    nation_customer_part = v1
    build_side = nation_customer_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_customer_probe.sum(lambda x: ({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].c_nationkey] != None) else (None))

    nation_customer_orders_part = v0
    build_side = nation_customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    v0 = nation_customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
                                                      : True}) if (build_side[x[0].o_custkey] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"n2_name": x[0].n_name})): x[1]})

    nation_customer_orders_lineitem_part = v1
    build_side = nation_customer_orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = nation_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    nation_supplier_nation_customer_orders_lineitem_probe = v0
    nation_supplier_probe = su
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))

    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"n1_name": x[0].n_name})): x[1]})

    nation_supplier_nation_customer_orders_lineitem_part = v1
    build_side = nation_supplier_nation_customer_orders_lineitem_part.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_nation_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_suppkey] != None) else (None))

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (((((x[0].n1_name == france) * (x[0].n2_name == germany))) + (
    ((x[0].n1_name == germany) * (x[0].n2_name == france))))) else (None))

    v2 = v1.sum(lambda x: {x[0].concat(record({"supp_nation": x[0].n1_name})): x[1]})

    v3 = v2.sum(lambda x: {x[0].concat(record({"cust_nation": x[0].n2_name})): x[1]})

    v4 = v3.sum(lambda x: {x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]})

    v5 = v4.sum(
        lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v6 = v5.sum(lambda x: {
        record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record(
            {"revenue": x[0].volume})})

    v7 = v6.sum(lambda x: {x[0].concat(x[1]): True})

    results = v7

    return results


######

@sdql_compile({"pa": part_type, "su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type,
               "na": nation_type, "re": region_type})
def q8(pa, su, li, ord, cu, na, re):
    economyanodizedsteel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"
    region_n1_customer_orders_lineitem_probe = li
    v0 = ord.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19950101) * (x[0].o_orderdate <= 19961231))) else (None))

    region_n1_customer_orders_probe = v0
    region_n1_customer_probe = cu
    v0 = na.sum(lambda x: {x[0].concat(record({"n1_nationkey": x[0].n_nationkey})): x[1]})

    v1 = v0.sum(lambda x: {x[0].concat(record({"n1_name": x[0].n_name})): x[1]})

    v2 = v1.sum(lambda x: {x[0].concat(record({"n1_regionkey": x[0].n_regionkey})): x[1]})

    v3 = v2.sum(lambda x: {x[0].concat(record({"n1_comment": x[0].n_comment})): x[1]})

    region_n1_probe = v3
    v0 = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == america) else (None))

    region_n1_part = v0
    build_side = region_n1_part.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    v0 = region_n1_probe.sum(lambda x: ({build_side[x[0].n1_regionkey].sum(lambda y: x[0].concat(y[0]))
                                         : True}) if (build_side[x[0].n1_regionkey] != None) else (None))

    region_n1_customer_part = v0
    build_side = region_n1_customer_part.sum(lambda x: {x[0].n1_nationkey: sr_dict({x[0]: x[1]})})

    v0 = region_n1_customer_probe.sum(lambda x: ({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
                                                  : True}) if (build_side[x[0].c_nationkey] != None) else (None))

    region_n1_customer_orders_part = v0
    build_side = region_n1_customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    v0 = region_n1_customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
                                                         : True}) if (build_side[x[0].o_custkey] != None) else (None))

    region_n1_customer_orders_lineitem_part = v0
    build_side = region_n1_customer_orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = region_n1_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    part_region_n1_customer_orders_lineitem_probe = v0
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (x[0].p_type == economyanodizedsteel) else (None))

    part_region_n1_customer_orders_lineitem_part = v0
    build_side = part_region_n1_customer_orders_lineitem_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_region_n1_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_partkey] != None) else (None))

    supplier_part_region_n1_customer_orders_lineitem_probe = v0
    supplier_part_region_n1_customer_orders_lineitem_part = su
    build_side = supplier_part_region_n1_customer_orders_lineitem_part.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = supplier_part_region_n1_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_suppkey] != None) else (None))

    n2_supplier_part_region_n1_customer_orders_lineitem_probe = v0
    v0 = na.sum(lambda x: {x[0].concat(record({"n2_nationkey": x[0].n_nationkey})): x[1]})

    v1 = v0.sum(lambda x: {x[0].concat(record({"n2_name": x[0].n_name})): x[1]})

    v2 = v1.sum(lambda x: {x[0].concat(record({"n2_regionkey": x[0].n_regionkey})): x[1]})

    v3 = v2.sum(lambda x: {x[0].concat(record({"n2_comment": x[0].n_comment})): x[1]})

    n2_supplier_part_region_n1_customer_orders_lineitem_part = v3
    build_side = n2_supplier_part_region_n1_customer_orders_lineitem_part.sum(
        lambda x: {x[0].n2_nationkey: sr_dict({x[0]: x[1]})})

    v0 = n2_supplier_part_region_n1_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})

    v2 = v1.sum(
        lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v3 = v2.sum(lambda x: {x[0].concat(record({"nation": x[0].n2_name})): x[1]})

    v4 = v3.sum(lambda x: {x[0].concat(record({"volume_A": (
    ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) if (x[0].nation == brazil) else (0.0)})): x[1]})

    v5 = v4.sum(lambda x: {record({"o_year": x[0].o_year}): record({"A": x[0].volume_A, "B": x[0].volume})})

    v6 = v5.sum(lambda x: {x[0].concat(x[1]): True})

    v7 = v6.sum(lambda x: {x[0].concat(record({"mkt_share": ((x[0].A) / (x[0].B))})): x[1]})

    results = v7

    return results


######

@sdql_compile({"li": lineitem_type, "ord": order_type, "na": nation_type, "su": supplier_type, "pa": part_type,
               "ps": partsupp_type})
def q9(li, ord, na, su, pa, ps):
    g = "g"
    orders_lineitem_probe = li
    orders_lineitem_part = ord
    build_side = orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    nation_supplier_part_partsupp_orders_lineitem_probe = v0
    part_partsupp_probe = ps
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (firstIndex(x[0].p_name, g) != ((-1) * (1))) else (None))

    part_partsupp_part = v0
    build_side = part_partsupp_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].ps_partkey] != None) else (None))

    nation_supplier_part_partsupp_probe = v0
    nation_supplier_probe = su
    nation_supplier_part = na
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    nation_supplier_part_partsupp_part = v0
    build_side = nation_supplier_part_partsupp_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_part_partsupp_probe.sum(
        lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].ps_suppkey] != None) else (None))

    nation_supplier_part_partsupp_orders_lineitem_part = v0
    build_side = nation_supplier_part_partsupp_orders_lineitem_part.sum(
        lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey}): sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_part_partsupp_orders_lineitem_probe.sum(lambda x: (
    {build_side[record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey})].sum(lambda y: x[0].concat(y[0]))
     : True}) if (build_side[record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey})] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"nation": x[0].n_name})): x[1]})

    v2 = v1.sum(lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})

    v3 = v2.sum(lambda x: {x[0].concat(record({"amount": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) - (
    ((x[0].ps_supplycost) * (x[0].l_quantity))))})): x[1]})

    v4 = v3.sum(lambda x: {record({"nation": x[0].nation, "o_year": x[0].o_year}): record({"sum_profit": x[0].amount})})

    v5 = v4.sum(lambda x: {x[0].concat(x[1]): True})

    results = v5

    return results


######

@sdql_compile({"cu": customer_type, "ord": order_type, "li": lineitem_type, "na": nation_type})
def q10(cu, ord, li, na):
    r = "R"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_returnflag == r) else (None))

    nation_customer_orders_lineitem_probe = v0
    v0 = ord.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19931001) * (x[0].o_orderdate < 19940101))) else (None))

    customer_orders_probe = v0
    customer_orders_part = cu
    build_side = customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    v0 = customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].o_custkey] != None) else (None))

    nation_customer_orders_probe = v0
    nation_customer_orders_part = na
    build_side = nation_customer_orders_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_customer_orders_probe.sum(lambda x: ({build_side[x[0].c_nationkey].sum(lambda y: x[0].concat(y[0]))
                                                      : True}) if (build_side[x[0].c_nationkey] != None) else (None))

    nation_customer_orders_lineitem_part = v0
    build_side = nation_customer_orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = nation_customer_orders_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    v1 = v0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v2 = v1.sum(lambda x: {record(
        {"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_phone": x[0].c_phone,
         "n_name": x[0].n_name, "c_address": x[0].c_address, "c_comment": x[0].c_comment}): record(
        {"revenue": x[0].revenue})})

    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})

    results = v3

    return results


#######

@sdql_compile({"ps": partsupp_type, "su": supplier_type, "na": nation_type})
def q11(ps, su, na):
    germany = "GERMANY"
    nation_supplier_partsupp_probe = ps
    nation_supplier_probe = su
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))

    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    nation_supplier_partsupp_part = v0
    build_side = nation_supplier_partsupp_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_suppkey].sum(lambda y: x[0].concat(y[0]))
                                                        : True}) if (build_side[x[0].ps_suppkey] != None) else (None))

    tmp_calc_value = v0.sum(lambda x: ((((x[0].ps_supplycost) * (x[0].ps_availqty))) * (0.0001)))

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (tmp_calc_value < ((x[0].ps_supplycost) * (x[0].ps_availqty))) else (None))

    v2 = v1.sum(lambda x: {x[0].concat(record({"value": ((x[0].ps_supplycost) * (x[0].ps_availqty))})): x[1]})

    v3 = v2.sum(lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"value": x[0].value})})

    v4 = v3.sum(lambda x: {x[0].concat(x[1]): True})

    results = v4

    return results


######

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q12(ord, li):
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (((((((((((x[0].l_shipmode == ship) + (x[0].l_shipmode == mail))) * (
                x[0].l_commitdate < x[0].l_receiptdate))) * (x[0].l_shipdate < x[0].l_commitdate))) * (
                                                            x[0].l_receiptdate >= 19940101))) * (
                                                          x[0].l_receiptdate < 19950101))) else (None))

    orders_lineitem_probe = v0
    orders_lineitem_part = ord
    build_side = orders_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = orders_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"high_line_priority": (1) if (
    ((x[0].o_orderpriority == urgent1) + (x[0].o_orderpriority == high2))) else (0)})): x[1]})

    v2 = v1.sum(lambda x: {x[0].concat(record({"low_line_priority": (1) if (
    ((x[0].o_orderpriority != urgent1) * (x[0].o_orderpriority != high2))) else (0)})): x[1]})

    v3 = v2.sum(lambda x: {record({"l_shipmode": x[0].l_shipmode}): record(
        {"high_line_count": x[0].high_line_priority, "low_line_count": x[0].low_line_priority})})

    v4 = v3.sum(lambda x: {x[0].concat(x[1]): True})

    results = v4

    return results


######

@sdql_compile({"cu": customer_type, "ord": order_type})
def q13(cu, ord):
    special = "special"
    requests = "requests"
    orders_customer_probe = cu
    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].o_comment, special) != -1) * (
                firstIndex(x[0].o_comment, requests) > ((firstIndex(x[0].o_comment, special)) + (6)))) == False) else (
        None))

    orders_customer_part = v0
    build_side = orders_customer_part.sum(lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})

    v0 = orders_customer_probe.sum(lambda x: ({x[0]: True}) if (build_side[x[0].c_custkey] == None) else (
        build_side[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
        ))

    v1 = v0.sum(lambda x: {record({"c_custkey": x[0].c_custkey}): record({"c_count": 1.0})})

    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})

    v3 = v2.sum(lambda x: {record({"c_count": x[0].c_count}): record({"custdist": 1.0})})

    v4 = v3.sum(lambda x: {x[0].concat(x[1]): True})

    results = v4

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q14(li, pa):
    promo = "PROMO"
    v0 = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None))

    part_lineitem_probe = v0
    part_lineitem_part = pa
    build_side = part_lineitem_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].l_partkey] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"A": (((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) if (
        startsWith(x[0].p_type, promo)) else (0.0)})): x[1]})

    v2 = v1.sum(lambda x: {x[0].concat(record({"B": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v3 = v2.sum(lambda x: record({"A": x[0].A, "B": x[0].B}))

    v4 = ((((v3.A) * (100.0))) / (v3.B))
    results = v4

    return results


#######

@sdql_compile({"li": lineitem_type, "su": supplier_type})
def q15(li, su):
    v0 = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19960101) * (x[0].l_shipdate < 19960401))) else (None))

    v1 = v0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v2 = v1.sum(lambda x: {record({"l_suppkey": x[0].l_suppkey}): record({"total_revenue": x[0].revenue})})

    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})

    v4 = v3.sum(lambda x: ({x[0]: x[1]}) if (x[0].total_revenue == 1614410.2928) else (None))

    supplier_lineitem_probe = v4
    supplier_lineitem_part = su
    build_side = supplier_lineitem_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = supplier_lineitem_probe.sum(lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
                                                 : True}) if (build_side[x[0].l_suppkey] != None) else (None))

    results = v0

    return results


#######

@sdql_compile({"ps": partsupp_type, "pa": part_type, "su": supplier_type})
def q16(ps, pa, su):
    brand45 = "Brand#45"
    mediumpolished = "MEDIUM POLISHED"
    customer = "Customer"
    complaints = "Complaints"
    v0 = su.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].s_comment, customer) != ((-1) * (1))) * (
                firstIndex(x[0].s_comment, complaints) > ((firstIndex(x[0].s_comment, customer)) + (7))))) else (None))

    partsupp_supplier_isin_build = v0
    partsupp_supplier_isin_build = partsupp_supplier_isin_build.sum(lambda x: {x[0].s_suppkey: True})

    v0 = ps.sum(lambda x: ({x[0]: x[1]}) if (partsupp_supplier_isin_build[x[0].ps_suppkey] == None) else (None))

    part_partsupp_probe = v0
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if ((
                (((x[0].p_brand != brand45) * (startsWith(x[0].p_type, mediumpolished) == False))) * ((((((((
                    (((((((x[0].p_size == 9) + (x[0].p_size == 36))) + (x[0].p_size == 49))) + (x[0].p_size == 14))) + (
                        x[0].p_size == 23))) + (x[0].p_size == 45))) + (x[0].p_size == 19))) + (x[
                                                                                                    0].p_size == 3))))) else (
        None))

    part_partsupp_part = v0
    build_side = part_partsupp_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_partsupp_probe.sum(lambda x: ({build_side[x[0].ps_partkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].ps_partkey] != None) else (None))

    v1 = v0.sum(lambda x: {
        record({"p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size}): record({"supplier_cnt": 1.0})})

    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})

    results = v2

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q17(li, pa):
    brand11 = "Brand#11"
    wrapcase = "WRAP CASE"
    part_l1_lineitem_probe = li
    v0 = li.sum(
        lambda x: {record({"l_partkey": x[0].l_partkey}): record({"sum_quant": x[0].l_quantity, "count_quant": 1.0})})

    v1 = v0.sum(lambda x: {x[0].concat(x[1]): True})

    part_l1_probe = v1
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((x[0].p_brand == brand11) * (x[0].p_container == wrapcase))) else (None))

    part_l1_part = v0
    build_side = part_l1_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_l1_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
                                       : True}) if (build_side[x[0].l_partkey] != None) else (None))

    part_l1_lineitem_part = v0
    build_side = part_l1_lineitem_part.sum(lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_l1_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
                                                : True}) if (build_side[x[0].l_partkey] != None) else (None))

    v1 = v0.sum(lambda x: {x[0].concat(record({"l_extendedprice": (x[0].l_extendedprice) if (
                x[0].l_quantity < ((0.2) * (((x[0].sum_quant) / (x[0].count_quant))))) else (0.0)})): x[1]})

    v2 = v1.sum(lambda x: record({"l_extendedprice": x[0].l_extendedprice}))

    v3 = ((v2.l_extendedprice) / (7.0))
    results = v3

    return results


#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q18(li, cu, ord):
    customer_orders_l1_probe = li
    v0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"sum_quantity": x[0].l_quantity})})

    v1 = v0.sum(lambda x: {x[0].concat(x[1]): True})

    v2 = v1.sum(lambda x: ({x[0]: x[1]}) if (x[0].sum_quantity > 300) else (None))

    orders_lineitem_isin_build = v2
    orders_lineitem_isin_build = orders_lineitem_isin_build.sum(lambda x: {x[0].l_orderkey: True})

    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (orders_lineitem_isin_build[x[0].o_orderkey] != None) else (None))

    customer_orders_probe = v0
    customer_orders_part = cu
    build_side = customer_orders_part.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    v0 = customer_orders_probe.sum(lambda x: ({build_side[x[0].o_custkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].o_custkey] != None) else (None))

    customer_orders_l1_part = v0
    build_side = customer_orders_l1_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = customer_orders_l1_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                                                  : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    v1 = v0.sum(lambda x: {record({"c_name": x[0].c_name, "c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey,
                                   "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice}): record(
        {"sum_quantity": x[0].l_quantity})})

    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})

    results = v2

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q19(li, pa):
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
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (
    ((((x[0].l_shipmode == air) + (x[0].l_shipmode == airreg))) * (x[0].l_shipinstruct == deliverinperson))) else (
        None))

    part_lineitem_probe = v0
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((((((((((x[0].p_brand == brand12) * ((
                (((((x[0].p_container == smpkg) + (x[0].p_container == smpack))) + (x[0].p_container == smcase))) + (
                    x[0].p_container == smbox))))) * (x[0].p_size >= 1))) * (x[0].p_size <= 5))) + ((((((
                (x[0].p_brand == brand23) * (((((((x[0].p_container == medpack) + (x[0].p_container == medpkg))) + (
                    x[0].p_container == medbag))) + (x[0].p_container == medbox))))) * (x[0].p_size >= 1))) * (x[
                                                                                                                   0].p_size <= 10))))) + (
                                              ((((((x[0].p_brand == brand34) * (((((((x[0].p_container == lgpkg) + (
                                                          x[0].p_container == lgpack))) + (x[
                                                                                               0].p_container == lgcase))) + (
                                                                                             x[
                                                                                                 0].p_container == lgbox))))) * (
                                                             x[0].p_size >= 1))) * (x[0].p_size <= 15))))) else (None))

    part_lineitem_part = v0
    build_side = part_lineitem_part.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    v0 = part_lineitem_probe.sum(lambda x: ({build_side[x[0].l_partkey].sum(lambda y: x[0].concat(y[0]))
                                             : True}) if (build_side[x[0].l_partkey] != None) else (None))

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if ((((
                (((x[0].p_brand == brand12) * (((x[0].l_quantity >= 1) * (x[0].l_quantity <= 11))))) + (
        ((x[0].p_brand == brand23) * (((x[0].l_quantity >= 10) * (x[0].l_quantity <= 20))))))) + ((
                (x[0].p_brand == brand34) * (((x[0].l_quantity >= 20) * (x[0].l_quantity <= 30))))))) else (None))

    v2 = v1.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    v3 = v2.sum(lambda x: record({"revenue": x[0].revenue}))

    v4 = v3.sum(lambda x: {v3: True})

    results = v4

    return results


#######

@sdql_compile({"su": supplier_type, "na": nation_type, "ps": partsupp_type, "pa": part_type, "li": lineitem_type})
def q20(su, na, ps, pa, li):
    canada = "CANADA"
    forest = "forest"
    lineitem_supplier_isin_build = su
    lineitem_partsupp_probe = ps
    v0 = pa.sum(lambda x: ({x[0]: x[1]}) if (startsWith(x[0].p_name, forest)) else (None))

    lineitem_part_isin_build = v0
    lineitem_supplier_isin_build = su
    v0 = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) else (None))

    lineitem_part_isin_build = lineitem_part_isin_build.sum(lambda x: {x[0].p_partkey: True})

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (lineitem_part_isin_build[x[0].l_partkey] != None) else (None))

    lineitem_supplier_isin_build = su.sum(lambda x: {x[0].s_suppkey: True})

    v2 = v1.sum(lambda x: ({x[0]: x[1]}) if (lineitem_supplier_isin_build[x[0].l_suppkey] != None) else (None))

    v3 = v2.sum(lambda x: {
        record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): record({"sum_quantity": x[0].l_quantity})})

    v4 = v3.sum(lambda x: {x[0].concat(x[1]): True})

    lineitem_partsupp_part = v4
    build_side = lineitem_partsupp_part.sum(
        lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): sr_dict({x[0]: x[1]})})

    v0 = lineitem_partsupp_probe.sum(lambda x: (
    {build_side[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})].sum(lambda y: x[0].concat(y[0]))
     : True}) if (build_side[record({"ps_partkey": x[0].ps_partkey, "ps_suppkey": x[0].ps_suppkey})] != None) else (
        None))

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (x[0].ps_availqty > ((x[0].sum_quantity) * (0.5))) else (None))

    supplier_lineitem_partsupp_isin_build = v1
    supplier_lineitem_partsupp_isin_build = supplier_lineitem_partsupp_isin_build.sum(lambda x: {x[0].l_suppkey: True})

    v0 = su.sum(lambda x: ({x[0]: x[1]}) if (supplier_lineitem_partsupp_isin_build[x[0].s_suppkey] != None) else (None))

    nation_supplier_probe = v0
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == canada) else (None))

    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    results = v0

    return results


#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "na": nation_type})
def q21(su, li, ord, na):
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))

    l2_lineitem_probe = v0
    v0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"l2_size": 1.0})})

    v1 = v0.sum(lambda x: {x[0].concat(x[1]): True})

    l2_lineitem_part = v1
    build_side = l2_lineitem_part.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})

    v0 = l2_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                                           : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    l3_l2_lineitem_probe = v0
    v0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))

    v1 = v0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"l3_size": 1.0})})

    v2 = v1.sum(lambda x: {x[0].concat(x[1]): True})

    l3_l2_lineitem_part = v2
    build_side = l3_l2_lineitem_part.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})

    v0 = l3_l2_lineitem_probe.sum(lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                                              : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    nation_supplier_l3_l2_lineitem_probe = v0
    nation_supplier_probe = su
    v0 = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == saudiarabia) else (None))

    nation_supplier_part = v0
    build_side = nation_supplier_part.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_probe.sum(lambda x: ({build_side[x[0].s_nationkey].sum(lambda y: x[0].concat(y[0]))
                                               : True}) if (build_side[x[0].s_nationkey] != None) else (None))

    nation_supplier_l3_l2_lineitem_part = v0
    build_side = nation_supplier_l3_l2_lineitem_part.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    v0 = nation_supplier_l3_l2_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_suppkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_suppkey] != None) else (None))

    orders_nation_supplier_l3_l2_lineitem_probe = v0
    v0 = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderstatus == f) else (None))

    orders_nation_supplier_l3_l2_lineitem_part = v0
    build_side = orders_nation_supplier_l3_l2_lineitem_part.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    v0 = orders_nation_supplier_l3_l2_lineitem_probe.sum(
        lambda x: ({build_side[x[0].l_orderkey].sum(lambda y: x[0].concat(y[0]))
                    : True}) if (build_side[x[0].l_orderkey] != None) else (None))

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (((x[0].l2_size > 1) * (x[0].l3_size == 1))) else (None))

    v2 = v1.sum(lambda x: {record({"s_name": x[0].s_name}): record({"numwait": 1.0})})

    v3 = v2.sum(lambda x: {x[0].concat(x[1]): True})

    results = v3

    return results


#####

@sdql_compile({"cu": customer_type, "ord": order_type})
def q22(cu, ord):
    v13 = "13"
    v31 = "31"
    v23 = "23"
    v29 = "29"
    v30 = "30"
    v18 = "18"
    v17 = "17"
    customer_orders_isin_build = ord
    v0 = cu.sum(lambda x: ({x[0]: x[1]}) if (((x[0].c_acctbal > 0.0) * ((((((((((
                (((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (
            startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone, v30)))) + (
                                                                                startsWith(x[0].c_phone, v18)))) + (
                                                                              startsWith(x[0].c_phone, v17)))))) else (
        None))

    v1 = v0.sum(lambda x: record({"sum_acctbal": x[0].c_acctbal, "count_acctbal": 1.0}))

    v2 = v1.sum(lambda x: {v1: True})

    prev_aggregation = v1
    v0 = cu.sum(lambda x: ({x[0]: x[1]}) if ((
                (x[0].c_acctbal > ((prev_aggregation.sum_acctbal) / (prev_aggregation.count_acctbal))) * ((((((((((
                    (((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (
                startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone,
                                                                                                   v30)))) + (
                                                                                                                 startsWith(
                                                                                                                     x[
                                                                                                                         0].c_phone,
                                                                                                                     v18)))) + (
                                                                                                               startsWith(
                                                                                                                   x[
                                                                                                                       0].c_phone,
                                                                                                                   v17)))))) else (
        None))

    customer_orders_isin_build = ord.sum(lambda x: {x[0].o_custkey: True})

    v1 = v0.sum(lambda x: ({x[0]: x[1]}) if (customer_orders_isin_build[x[0].c_custkey] == None) else (None))

    v2 = v1.sum(lambda x: {x[0].concat(record({"cntrycode": substr(x[0].c_phone, 0, 1)})): x[1]})

    v3 = v2.sum(
        lambda x: {record({"cntrycode": x[0].cntrycode}): record({"numcust": 1.0, "totacctbal": x[0].c_acctbal})})

    v4 = v3.sum(lambda x: {x[0].concat(x[1]): True})

    results = v4

    return results


######### Function Calls

benchmark("Q1", iterations, q1, [lineitem], show_results, verbose)
benchmark("Q2", iterations, q2, [part, supplier, partsupp, nation, region], show_results, verbose)
benchmark("Q3", iterations, q3, [lineitem, customer, order], show_results, verbose)
benchmark("Q4", iterations, q4, [order, lineitem], show_results, verbose)
benchmark("Q5", iterations, q5, [lineitem, customer, order, region, nation, supplier], show_results, verbose)
benchmark("Q6", iterations, q6, [lineitem], show_results, verbose)
benchmark("Q7", iterations, q7, [supplier, lineitem, order, customer, nation], show_results, verbose)
benchmark("Q8", iterations, q8, [part, supplier, lineitem, order, customer, nation, region], show_results, verbose)
benchmark("Q9", iterations, q9, [lineitem, order, nation, supplier, part, partsupp], show_results, verbose)
benchmark("Q10", iterations, q10, [customer, order, lineitem, nation], show_results, verbose)
benchmark("Q11", iterations, q11, [partsupp, supplier, nation], show_results, verbose)
benchmark("Q12", iterations, q12, [order, lineitem], show_results, verbose)
benchmark("Q13", iterations, q13, [customer, order], show_results, verbose)
benchmark("Q14", iterations, q14, [lineitem, part], show_results, verbose)
benchmark("Q15", iterations, q15, [lineitem, supplier], show_results, verbose)
benchmark("Q16", iterations, q16, [partsupp, part, supplier], show_results, verbose)
benchmark("Q17", iterations, q17, [lineitem, part], show_results, verbose)
benchmark("Q18", iterations, q18, [lineitem, customer, order], show_results, verbose)
benchmark("Q19", iterations, q19, [lineitem, part], show_results, verbose)
benchmark("Q20", iterations, q20, [supplier, nation, partsupp, part, lineitem], show_results, verbose)
benchmark("Q21", iterations, q21, [supplier, lineitem, order, nation], show_results, verbose)
benchmark("Q22", iterations, q22, [customer, order], show_results, verbose)