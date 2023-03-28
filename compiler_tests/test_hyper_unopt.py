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
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate <= 19980902) else (None))

    lineitem_1 = lineitem_0.sum(
        lambda x: {x[0].concat(record({"disc_price": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    lineitem_2 = lineitem_1.sum(lambda x: {x[0].concat(
        record({"charge": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) * (((1.0) + (x[0].l_tax))))})): x[
        1]})

    lineitem_3 = lineitem_2.sum(lambda x: {
        record({"l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus}): record(
            {"sum_qty": x[0].l_quantity, "sum_base_price": x[0].l_extendedprice, "sum_disc_price": x[0].disc_price,
             "sum_charge": x[0].charge, "avg_qty_sum_for_mean": x[0].l_quantity, "avg_qty_count_for_mean": 1.0,
             "avg_price_sum_for_mean": x[0].l_extendedprice, "avg_price_count_for_mean": 1.0,
             "avg_disc_sum_for_mean": x[0].l_discount, "avg_disc_count_for_mean": 1.0,
             "count_order": (1.0) if (x[0].l_quantity != None) else (0.0)})})

    results = lineitem_3.sum(lambda x: {record(
        {"l_returnflag": x[0].l_returnflag, "l_linestatus": x[0].l_linestatus, "sum_qty": x[1].sum_qty,
         "sum_base_price": x[1].sum_base_price, "sum_disc_price": x[1].sum_disc_price, "sum_charge": x[1].sum_charge,
         "avg_qty": ((x[1].avg_qty_sum_for_mean) / (x[1].avg_qty_count_for_mean)),
         "avg_price": ((x[1].avg_price_sum_for_mean) / (x[1].avg_price_count_for_mean)),
         "avg_disc": ((x[1].avg_disc_sum_for_mean) / (x[1].avg_disc_count_for_mean)),
         "count_order": x[1].count_order}): True})

    return results


######

@sdql_compile({"pa": part_type, "su": supplier_type, "ps": partsupp_type, "na": nation_type, "re": region_type})
def q2(pa, su, ps, na, re):
    europe = "EUROPE"
    brass = "BRASS"
    region_nation_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))

    region_nation_build_nest_dict = region_nation_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    region_nation_0 = na.sum(
        lambda x: (region_nation_build_nest_dict[x[0].n_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_build_nest_dict[x[0].n_regionkey] != None) else (None))

    region_nation_supplier_build_pre_ops = region_nation_0.sum(
        lambda x: {record({"n_nationkey": x[0].n_nationkey, "n_name": x[0].n_name}): True})

    region_nation_supplier_build_nest_dict = region_nation_supplier_build_pre_ops.sum(
        lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    region_nation_supplier_0 = su.sum(
        lambda x: (region_nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_pre_ops = region_nation_supplier_0.sum(
        lambda x: {record(
            {"s_suppkey": x[0].s_suppkey, "s_acctbal": x[0].s_acctbal, "s_name": x[0].s_name, "n_name": x[0].n_name,
             "s_address": x[0].s_address, "s_phone": x[0].s_phone, "s_comment": x[0].s_comment}): True})

    region_nation_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == europe) else (None))

    region_nation_build_nest_dict = region_nation_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    region_nation_0 = na.sum(
        lambda x: (region_nation_build_nest_dict[x[0].n_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_build_nest_dict[x[0].n_regionkey] != None) else (None))

    region_nation_supplier_build_pre_ops = region_nation_0.sum(
        lambda x: {record({"n_nationkey": x[0].n_nationkey, "n_name": x[0].n_name}): True})

    region_nation_supplier_build_nest_dict = region_nation_supplier_build_pre_ops.sum(
        lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    region_nation_supplier_0 = su.sum(
        lambda x: (region_nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    region_nation_supplier_ps1_build_pre_ops = region_nation_supplier_0.sum(lambda x: {record(
        {"s_suppkey": x[0].s_suppkey, "s_acctbal": x[0].s_acctbal, "s_name": x[0].s_name, "n_name": x[0].n_name,
         "s_address": x[0].s_address, "s_phone": x[0].s_phone, "s_comment": x[0].s_comment}): True})

    region_nation_supplier_ps1_build_nest_dict = region_nation_supplier_ps1_build_pre_ops.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    region_nation_supplier_ps1_0 = ps.sum(
        lambda x: (region_nation_supplier_ps1_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_supplier_ps1_build_nest_dict[x[0].ps_suppkey] != None) else (None))

    region_nation_supplier_ps1_1 = region_nation_supplier_ps1_0.sum(
        lambda x: {record({"ps_partkey": x[0].ps_partkey, "ps_supplycost": x[0].ps_supplycost}): True})

    region_nation_supplier_ps1_2 = region_nation_supplier_ps1_1.sum(
        lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"min_supplycost": x[0].ps_supplycost})})

    region_nation_supplier_ps1_part_partsupp_build_pre_ops = region_nation_supplier_ps1_2.sum(
        lambda x: {x[0].concat(x[1]): True})

    part_0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((endsWith(x[0].p_type, brass)) * (x[0].p_size == 15))) else (None))

    part_partsupp_build_pre_ops = part_0.sum(
        lambda x: {record({"p_partkey": x[0].p_partkey, "p_mfgr": x[0].p_mfgr}): True})

    part_partsupp_build_nest_dict = part_partsupp_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    region_nation_supplier_ps1_part_partsupp_probe_pre_ops = ps.sum(
        lambda x: (part_partsupp_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))

    region_nation_supplier_ps1_part_partsupp_build_nest_dict = region_nation_supplier_ps1_part_partsupp_build_pre_ops.sum(
        lambda x: {x[0].ps_partkey: sr_dict({x[0]: x[1]})})

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe_pre_ops = region_nation_supplier_ps1_part_partsupp_probe_pre_ops.sum(
        lambda x: (region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_partkey].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_pre_ops.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_0 = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe_pre_ops.sum(
        lambda x: (region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict[x[0].ps_suppkey].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict[
                             x[0].ps_suppkey] != None) else (None))

    region_nation_supplier_region_nation_supplier_ps1_part_partsupp_1 = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_0.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].ps_supplycost == x[0].min_supplycost) else (None))

    results = region_nation_supplier_region_nation_supplier_ps1_part_partsupp_1.sum(lambda x: {record(
        {"s_acctbal": x[0].s_acctbal, "s_name": x[0].s_name, "n_name": x[0].n_name, "p_partkey": x[0].p_partkey,
         "p_mfgr": x[0].p_mfgr, "s_address": x[0].s_address, "s_phone": x[0].s_phone,
         "s_comment": x[0].s_comment}): True})

    return results


######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q3(li, cu, ord):
    building = "BUILDING"
    customer_orders_build_pre_ops = cu.sum(lambda x: ({x[0]: x[1]}) if (x[0].c_mktsegment == building) else (None))

    customer_orders_probe_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (x[0].o_orderdate < 19950315) else (None))

    customer_orders_build_nest_dict = customer_orders_build_pre_ops.sum(
        lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    customer_orders_lineitem_build_pre_ops = customer_orders_probe_pre_ops.sum(
        lambda x: (customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))

    customer_orders_lineitem_probe_pre_ops = li.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].l_shipdate > 19950315) else (None))

    customer_orders_lineitem_build_nest_dict = customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    customer_orders_lineitem_0 = customer_orders_lineitem_probe_pre_ops.sum(
        lambda x: (customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    customer_orders_lineitem_1 = customer_orders_lineitem_0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    customer_orders_lineitem_2 = customer_orders_lineitem_1.sum(lambda x: {record(
        {"l_orderkey": x[0].l_orderkey, "o_orderdate": x[0].o_orderdate,
         "o_shippriority": x[0].o_shippriority}): record({"revenue": x[0].revenue})})

    results = customer_orders_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})

    return results


#####

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q4(ord, li):
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_commitdate < x[0].l_receiptdate) else (None))

    lineitem_orders_isin_pre_ops = lineitem_0.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): True})

    lineitem_orders_isin_build_index = lineitem_orders_isin_pre_ops.sum(lambda x: {x[0].l_orderkey: True})

    orders_0 = ord.sum(
        lambda x: ({x[0]: x[1]}) if (lineitem_orders_isin_build_index[x[0].o_orderkey] != None) else (None))

    orders_1 = orders_0.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19930701) * (x[0].o_orderdate < 19931001))) else (None))

    orders_2 = orders_1.sum(lambda x: {record({"o_orderpriority": x[0].o_orderpriority}): record(
        {"order_count": (1.0) if (x[0].o_orderdate != None) else (0.0)})})

    results = orders_2.sum(lambda x: {x[0].concat(x[1]): True})

    return results


#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type, "re": region_type, "na": nation_type,
               "su": supplier_type})
def q5(li, cu, ord, re, na, su):
    asia = "ASIA"
    region_nation_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == asia) else (None))

    region_nation_build_nest_dict = region_nation_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    region_nation_customer_build_pre_ops = na.sum(
        lambda x: (region_nation_build_nest_dict[x[0].n_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_build_nest_dict[x[0].n_regionkey] != None) else (None))

    region_nation_customer_build_nest_dict = region_nation_customer_build_pre_ops.sum(
        lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    region_nation_customer_orders_build_pre_ops = cu.sum(
        lambda x: (region_nation_customer_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_nation_customer_build_nest_dict[x[0].c_nationkey] != None) else (None))

    region_nation_customer_orders_probe_pre_ops = ord.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19940101) * (x[0].o_orderdate < 19950101))) else (None))

    region_nation_customer_orders_build_nest_dict = region_nation_customer_orders_build_pre_ops.sum(
        lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    region_nation_customer_orders_lineitem_build_pre_ops = region_nation_customer_orders_probe_pre_ops.sum(lambda x: (
        region_nation_customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (region_nation_customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))

    region_nation_customer_orders_lineitem_build_nest_dict = region_nation_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    supplier_region_nation_customer_orders_lineitem_probe_pre_ops = li.sum(lambda x: (
        region_nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (region_nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    supplier_region_nation_customer_orders_lineitem_build_nest_dict = su.sum(
        lambda x: {record({"s_suppkey": x[0].s_suppkey, "s_nationkey": x[0].s_nationkey}): sr_dict({x[0]: x[1]})})

    supplier_region_nation_customer_orders_lineitem_0 = supplier_region_nation_customer_orders_lineitem_probe_pre_ops.sum(
        lambda x: (supplier_region_nation_customer_orders_lineitem_build_nest_dict[
                       record({"s_suppkey": x[0].l_suppkey, "s_nationkey": x[0].c_nationkey})].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (supplier_region_nation_customer_orders_lineitem_build_nest_dict[
                             record({"s_suppkey": x[0].l_suppkey, "s_nationkey": x[0].c_nationkey})] != None) else (
            None))

    supplier_region_nation_customer_orders_lineitem_1 = supplier_region_nation_customer_orders_lineitem_0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    supplier_region_nation_customer_orders_lineitem_2 = supplier_region_nation_customer_orders_lineitem_1.sum(
        lambda x: {record({"n_name": x[0].n_name}): record({"revenue": x[0].revenue})})

    results = supplier_region_nation_customer_orders_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})

    return results


#######

@sdql_compile({"li": lineitem_type})
def q6(li):
    lineitem_0 = li.sum(lambda x: ({x[0]: x[1]}) if ((((
                (((((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) * (x[0].l_discount >= 0.05))) * (
                    x[0].l_discount <= 0.07))) * (x[0].l_quantity < 24))) else (None))

    lineitem_1 = lineitem_0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (x[0].l_discount))})): x[1]})

    lineitem_2 = lineitem_1.sum(lambda x: record({"revenue": x[0].revenue}))

    results = {lineitem_2: True}

    return results


#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type, "na": nation_type})
def q7(su, li, ord, cu, na):
    france = "FRANCE"
    germany = "GERMANY"
    n1_supplier_build_pre_ops = na.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))

    n1_supplier_build_nest_dict = n1_supplier_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    n1_supplier_0 = su.sum(
        lambda x: (n1_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (n1_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    n1_supplier_n2_customer_orders_lineitem_build_pre_ops = n1_supplier_0.sum(
        lambda x: {x[0].concat(record({"n1_name": x[0].n_name})): x[1]})

    n2_customer_build_pre_ops = na.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].n_name == france) + (x[0].n_name == germany))) else (None))

    n2_customer_build_nest_dict = n2_customer_build_pre_ops.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    n2_customer_orders_build_pre_ops = cu.sum(
        lambda x: (n2_customer_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (n2_customer_build_nest_dict[x[0].c_nationkey] != None) else (None))

    n2_customer_orders_build_nest_dict = n2_customer_orders_build_pre_ops.sum(
        lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    n2_customer_orders_0 = ord.sum(
        lambda x: (n2_customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (n2_customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))

    n2_customer_orders_lineitem_build_pre_ops = n2_customer_orders_0.sum(
        lambda x: {x[0].concat(record({"n2_name": x[0].n_name})): x[1]})

    n2_customer_orders_lineitem_probe_pre_ops = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950101) * (x[0].l_shipdate <= 19961231))) else (None))

    n2_customer_orders_lineitem_build_nest_dict = n2_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    n1_supplier_n2_customer_orders_lineitem_probe_pre_ops = n2_customer_orders_lineitem_probe_pre_ops.sum(
        lambda x: (n2_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (n2_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    n1_supplier_n2_customer_orders_lineitem_build_nest_dict = n1_supplier_n2_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    n1_supplier_n2_customer_orders_lineitem_0 = n1_supplier_n2_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (
        n1_supplier_n2_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (n1_supplier_n2_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))

    n1_supplier_n2_customer_orders_lineitem_1 = n1_supplier_n2_customer_orders_lineitem_0.sum(
        lambda x: ({x[0]: x[1]}) if (((((x[0].n1_name == france) * (x[0].n2_name == germany))) + (
        ((x[0].n1_name == germany) * (x[0].n2_name == france))))) else (None))

    n1_supplier_n2_customer_orders_lineitem_2 = n1_supplier_n2_customer_orders_lineitem_1.sum(
        lambda x: {x[0].concat(record({"supp_nation": x[0].n1_name})): x[1]})

    n1_supplier_n2_customer_orders_lineitem_3 = n1_supplier_n2_customer_orders_lineitem_2.sum(
        lambda x: {x[0].concat(record({"cust_nation": x[0].n2_name})): x[1]})

    n1_supplier_n2_customer_orders_lineitem_4 = n1_supplier_n2_customer_orders_lineitem_3.sum(
        lambda x: {x[0].concat(record({"l_year": extractYear(x[0].l_shipdate)})): x[1]})

    n1_supplier_n2_customer_orders_lineitem_5 = n1_supplier_n2_customer_orders_lineitem_4.sum(
        lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    n1_supplier_n2_customer_orders_lineitem_6 = n1_supplier_n2_customer_orders_lineitem_5.sum(lambda x: {record(
        {"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year,
         "volume": x[0].volume}): True})

    n1_supplier_n2_customer_orders_lineitem_7 = n1_supplier_n2_customer_orders_lineitem_6.sum(lambda x: {
        record({"supp_nation": x[0].supp_nation, "cust_nation": x[0].cust_nation, "l_year": x[0].l_year}): record(
            {"revenue": x[0].volume})})

    results = n1_supplier_n2_customer_orders_lineitem_7.sum(lambda x: {x[0].concat(x[1]): True})

    return results


######

@sdql_compile({"pa": part_type, "su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type,
               "na": nation_type, "re": region_type})
def q8(pa, su, li, ord, cu, na, re):
    economyanodizedsteel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"
    n2_0 = na.sum(lambda x: {x[0].concat(record({"n2_nationkey": x[0].n_nationkey})): x[1]})

    n2_1 = n2_0.sum(lambda x: {x[0].concat(record({"n2_name": x[0].n_name})): x[1]})

    n2_2 = n2_1.sum(lambda x: {x[0].concat(record({"n2_regionkey": x[0].n_regionkey})): x[1]})

    n2_supplier_part_region_n1_customer_orders_lineitem_build_pre_ops = n2_2.sum(
        lambda x: {x[0].concat(record({"n2_comment": x[0].n_comment})): x[1]})

    part_region_n1_customer_orders_lineitem_build_pre_ops = pa.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].p_type == economyanodizedsteel) else (None))

    region_n1_build_pre_ops = re.sum(lambda x: ({x[0]: x[1]}) if (x[0].r_name == america) else (None))

    n1_0 = na.sum(lambda x: {x[0].concat(record({"n1_nationkey": x[0].n_nationkey})): x[1]})

    n1_1 = n1_0.sum(lambda x: {x[0].concat(record({"n1_name": x[0].n_name})): x[1]})

    n1_2 = n1_1.sum(lambda x: {x[0].concat(record({"n1_regionkey": x[0].n_regionkey})): x[1]})

    region_n1_probe_pre_ops = n1_2.sum(lambda x: {x[0].concat(record({"n1_comment": x[0].n_comment})): x[1]})

    region_n1_build_nest_dict = region_n1_build_pre_ops.sum(lambda x: {x[0].r_regionkey: sr_dict({x[0]: x[1]})})

    region_n1_customer_build_pre_ops = region_n1_probe_pre_ops.sum(
        lambda x: (region_n1_build_nest_dict[x[0].n1_regionkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_n1_build_nest_dict[x[0].n1_regionkey] != None) else (None))

    region_n1_customer_build_nest_dict = region_n1_customer_build_pre_ops.sum(
        lambda x: {x[0].n1_nationkey: sr_dict({x[0]: x[1]})})

    region_n1_customer_orders_build_pre_ops = cu.sum(
        lambda x: (region_n1_customer_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_n1_customer_build_nest_dict[x[0].c_nationkey] != None) else (None))

    region_n1_customer_orders_probe_pre_ops = ord.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19950101) * (x[0].o_orderdate <= 19961231))) else (None))

    region_n1_customer_orders_build_nest_dict = region_n1_customer_orders_build_pre_ops.sum(
        lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    region_n1_customer_orders_lineitem_build_pre_ops = region_n1_customer_orders_probe_pre_ops.sum(
        lambda x: (region_n1_customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (region_n1_customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))

    region_n1_customer_orders_lineitem_build_nest_dict = region_n1_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    part_region_n1_customer_orders_lineitem_probe_pre_ops = li.sum(lambda x: (
        region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    part_region_n1_customer_orders_lineitem_build_nest_dict = part_region_n1_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops = part_region_n1_customer_orders_lineitem_probe_pre_ops.sum(
        lambda x: (part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_partkey].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))

    supplier_part_region_n1_customer_orders_lineitem_build_nest_dict = su.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    n2_supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops = supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops.sum(
        lambda x: (supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (
                    supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (
            None))

    n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict = n2_supplier_part_region_n1_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].n2_nationkey: sr_dict({x[0]: x[1]})})

    n2_supplier_part_region_n1_customer_orders_lineitem_0 = n2_supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops.sum(
        lambda x: (n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[x[0].s_nationkey].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict[
                             x[0].s_nationkey] != None) else (None))

    n2_supplier_part_region_n1_customer_orders_lineitem_1 = n2_supplier_part_region_n1_customer_orders_lineitem_0.sum(
        lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})

    n2_supplier_part_region_n1_customer_orders_lineitem_2 = n2_supplier_part_region_n1_customer_orders_lineitem_1.sum(
        lambda x: {x[0].concat(record({"volume": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    n2_supplier_part_region_n1_customer_orders_lineitem_3 = n2_supplier_part_region_n1_customer_orders_lineitem_2.sum(
        lambda x: {x[0].concat(record({"nation": x[0].n2_name})): x[1]})

    n2_supplier_part_region_n1_customer_orders_lineitem_4 = n2_supplier_part_region_n1_customer_orders_lineitem_3.sum(
        lambda x: {record({"o_year": x[0].o_year, "volume": x[0].volume, "nation": x[0].nation}): True})

    n2_supplier_part_region_n1_customer_orders_lineitem_5 = n2_supplier_part_region_n1_customer_orders_lineitem_4.sum(
        lambda x: {x[0].concat(record({"volume_A": (x[0].volume) if (x[0].nation == brazil) else (0.0)})): x[1]})

    n2_supplier_part_region_n1_customer_orders_lineitem_6 = n2_supplier_part_region_n1_customer_orders_lineitem_5.sum(
        lambda x: {record({"o_year": x[0].o_year}): record({"A": x[0].volume_A, "B": x[0].volume})})

    n2_supplier_part_region_n1_customer_orders_lineitem_7 = n2_supplier_part_region_n1_customer_orders_lineitem_6.sum(
        lambda x: {x[0].concat(x[1]): True})

    n2_supplier_part_region_n1_customer_orders_lineitem_8 = n2_supplier_part_region_n1_customer_orders_lineitem_7.sum(
        lambda x: {x[0].concat(record({"mkt_share": ((x[0].A) / (x[0].B))})): x[1]})

    results = n2_supplier_part_region_n1_customer_orders_lineitem_8.sum(
        lambda x: {record({"o_year": x[0].o_year, "mkt_share": x[0].mkt_share}): True})

    return results


######

@sdql_compile({"li": lineitem_type, "ord": order_type, "na": nation_type, "su": supplier_type, "pa": part_type,
               "ps": partsupp_type})
def q9(li, ord, na, su, pa, ps):
    green = "green"
    nation_supplier_build_nest_dict = na.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    nation_supplier_part_partsupp_build_pre_ops = su.sum(
        lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    part_partsupp_build_pre_ops = pa.sum(
        lambda x: ({x[0]: x[1]}) if (firstIndex(x[0].p_name, green) != ((-1) * (1))) else (None))

    part_partsupp_build_nest_dict = part_partsupp_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    nation_supplier_part_partsupp_probe_pre_ops = ps.sum(
        lambda x: (part_partsupp_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))

    nation_supplier_part_partsupp_build_nest_dict = nation_supplier_part_partsupp_build_pre_ops.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    nation_supplier_part_partsupp_orders_lineitem_build_pre_ops = nation_supplier_part_partsupp_probe_pre_ops.sum(
        lambda x: (
            nation_supplier_part_partsupp_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
            ) if (nation_supplier_part_partsupp_build_nest_dict[x[0].ps_suppkey] != None) else (None))

    orders_lineitem_build_nest_dict = ord.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    nation_supplier_part_partsupp_orders_lineitem_probe_pre_ops = li.sum(
        lambda x: (orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    nation_supplier_part_partsupp_orders_lineitem_build_nest_dict = nation_supplier_part_partsupp_orders_lineitem_build_pre_ops.sum(
        lambda x: {record({"ps_suppkey": x[0].ps_suppkey, "ps_partkey": x[0].ps_partkey}): sr_dict({x[0]: x[1]})})

    nation_supplier_part_partsupp_orders_lineitem_0 = nation_supplier_part_partsupp_orders_lineitem_probe_pre_ops.sum(
        lambda x: (nation_supplier_part_partsupp_orders_lineitem_build_nest_dict[
                       record({"ps_suppkey": x[0].l_suppkey, "ps_partkey": x[0].l_partkey})].sum(
            lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_supplier_part_partsupp_orders_lineitem_build_nest_dict[
                             record({"ps_suppkey": x[0].l_suppkey, "ps_partkey": x[0].l_partkey})] != None) else (None))

    nation_supplier_part_partsupp_orders_lineitem_1 = nation_supplier_part_partsupp_orders_lineitem_0.sum(
        lambda x: {x[0].concat(record({"nation": x[0].n_name})): x[1]})

    nation_supplier_part_partsupp_orders_lineitem_2 = nation_supplier_part_partsupp_orders_lineitem_1.sum(
        lambda x: {x[0].concat(record({"o_year": extractYear(x[0].o_orderdate)})): x[1]})

    nation_supplier_part_partsupp_orders_lineitem_3 = nation_supplier_part_partsupp_orders_lineitem_2.sum(lambda x: {
        x[0].concat(record({"amount": ((((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) - (
        ((x[0].ps_supplycost) * (x[0].l_quantity))))})): x[1]})

    nation_supplier_part_partsupp_orders_lineitem_4 = nation_supplier_part_partsupp_orders_lineitem_3.sum(
        lambda x: {record({"nation": x[0].nation, "o_year": x[0].o_year}): record({"sum_profit": x[0].amount})})

    results = nation_supplier_part_partsupp_orders_lineitem_4.sum(lambda x: {x[0].concat(x[1]): True})

    return results


######

@sdql_compile({"cu": customer_type, "ord": order_type, "li": lineitem_type, "na": nation_type})
def q10(cu, ord, li, na):
    r = "R"
    customer_orders_probe_pre_ops = ord.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].o_orderdate >= 19931001) * (x[0].o_orderdate < 19940101))) else (None))

    customer_orders_build_nest_dict = cu.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    nation_customer_orders_probe_pre_ops = customer_orders_probe_pre_ops.sum(
        lambda x: (customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))

    nation_customer_orders_build_nest_dict = na.sum(lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    nation_customer_orders_0 = nation_customer_orders_probe_pre_ops.sum(
        lambda x: (nation_customer_orders_build_nest_dict[x[0].c_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_customer_orders_build_nest_dict[x[0].c_nationkey] != None) else (None))

    nation_customer_orders_lineitem_build_pre_ops = nation_customer_orders_0.sum(lambda x: {record(
        {"o_orderkey": x[0].o_orderkey, "c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal,
         "c_phone": x[0].c_phone, "n_name": x[0].n_name, "c_address": x[0].c_address,
         "c_comment": x[0].c_comment}): True})

    nation_customer_orders_lineitem_probe_pre_ops = li.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].l_returnflag == r) else (None))

    nation_customer_orders_lineitem_build_nest_dict = nation_customer_orders_lineitem_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    nation_customer_orders_lineitem_0 = nation_customer_orders_lineitem_probe_pre_ops.sum(lambda x: (
        nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (nation_customer_orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    nation_customer_orders_lineitem_1 = nation_customer_orders_lineitem_0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    nation_customer_orders_lineitem_2 = nation_customer_orders_lineitem_1.sum(lambda x: {record(
        {"c_custkey": x[0].c_custkey, "c_name": x[0].c_name, "c_acctbal": x[0].c_acctbal, "c_phone": x[0].c_phone,
         "n_name": x[0].n_name, "c_address": x[0].c_address, "c_comment": x[0].c_comment}): record(
        {"revenue": x[0].revenue})})

    results = nation_customer_orders_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})

    return results


#######

@sdql_compile({"ps": partsupp_type, "su": supplier_type, "na": nation_type})
def q11(ps, su, na):
    germany = "GERMANY"
    nation_supplier_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == germany) else (None))

    nation_supplier_build_nest_dict = nation_supplier_build_pre_ops.sum(
        lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    nation_supplier_partsupp_build_pre_ops = su.sum(
        lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    nation_supplier_partsupp_build_nest_dict = nation_supplier_partsupp_build_pre_ops.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    nation_supplier_partsupp_0 = ps.sum(
        lambda x: (nation_supplier_partsupp_build_nest_dict[x[0].ps_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_supplier_partsupp_build_nest_dict[x[0].ps_suppkey] != None) else (None))

    tmp_var_JQ_JQ_ps_supplycost_mul_ps_availqty_XZ_mul_00001_XZ = nation_supplier_partsupp_0.sum(
        lambda x: ((((x[0].ps_supplycost) * (x[0].ps_availqty))) * (0.0001)))

    nation_supplier_partsupp_1 = nation_supplier_partsupp_0.sum(
        lambda x: {record({"ps_partkey": x[0].ps_partkey}): ((x[0].ps_supplycost) * (x[0].ps_availqty))})

    nation_supplier_partsupp_2 = nation_supplier_partsupp_1.sum(
        lambda x: ({x[0]: True}) if (tmp_var_JQ_JQ_ps_supplycost_mul_ps_availqty_XZ_mul_00001_XZ < x[1]) else (None))

    nation_supplier_partsupp_3 = nation_supplier_partsupp_0.sum(
        lambda x: ({x[0]: True}) if (nation_supplier_partsupp_2[record({"ps_partkey": x[0].ps_partkey})] != None) else (
            None))

    nation_supplier_partsupp_4 = nation_supplier_partsupp_3.sum(
        lambda x: {x[0].concat(record({"value": ((x[0].ps_supplycost) * (x[0].ps_availqty))})): x[1]})

    nation_supplier_partsupp_5 = nation_supplier_partsupp_4.sum(
        lambda x: {record({"ps_partkey": x[0].ps_partkey}): record({"value": x[0].value})})

    results = nation_supplier_partsupp_5.sum(lambda x: {x[0].concat(x[1]): True})

    return results


######

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q12(ord, li):
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    orders_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if ((((((((
                (((x[0].l_shipmode == ship) + (x[0].l_shipmode == mail))) * (
                    x[0].l_commitdate < x[0].l_receiptdate))) * (x[0].l_shipdate < x[0].l_commitdate))) * (x[
                                                                                                               0].l_receiptdate >= 19940101))) * (
                                                                                     x[
                                                                                         0].l_receiptdate < 19950101))) else (
        None))

    orders_lineitem_build_nest_dict = ord.sum(lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    orders_lineitem_0 = orders_lineitem_probe_pre_ops.sum(
        lambda x: (orders_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (orders_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    orders_lineitem_1 = orders_lineitem_0.sum(lambda x: {x[0].concat(record({"high_line_priority": (1) if (
    ((x[0].o_orderpriority == urgent1) + (x[0].o_orderpriority == high2))) else (0)})): x[1]})

    orders_lineitem_2 = orders_lineitem_1.sum(lambda x: {x[0].concat(record({"low_line_priority": (1) if (
    ((x[0].o_orderpriority != urgent1) * (x[0].o_orderpriority != high2))) else (0)})): x[1]})

    orders_lineitem_3 = orders_lineitem_2.sum(lambda x: {record({"l_shipmode": x[0].l_shipmode}): record(
        {"high_line_count": x[0].high_line_priority, "low_line_count": x[0].low_line_priority})})

    results = orders_lineitem_3.sum(lambda x: {x[0].concat(x[1]): True})

    return results


######

@sdql_compile({"cu": customer_type, "ord": order_type})
def q13(cu, ord):
    special = "special"
    requests = "requests"
    orders_customer_build_pre_ops = ord.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].o_comment,
                                                                                      special) != -1) * (
                                                                                      firstIndex(x[0].o_comment,
                                                                                                 requests) > ((
                                                                                                                  firstIndex(
                                                                                                                      x[
                                                                                                                          0].o_comment,
                                                                                                                      special)) + (
                                                                                                                  6)))) == False) else (
        None))

    orders_customer_build_nest_dict = orders_customer_build_pre_ops.sum(
        lambda x: {x[0].o_custkey: sr_dict({x[0]: x[1]})})

    orders_customer_0 = cu.sum(
        lambda x: ({x[0]: True}) if (orders_customer_build_nest_dict[x[0].c_custkey] == None) else (
            orders_customer_build_nest_dict[x[0].c_custkey].sum(lambda y: {x[0].concat(y[0]): True})
            ))

    orders_customer_1 = orders_customer_0.sum(lambda x: {
        record({"c_custkey": x[0].c_custkey}): record({"c_count": (1.0) if (x[0].o_orderkey != None) else (0.0)})})

    orders_customer_2 = orders_customer_1.sum(lambda x: {x[0].concat(x[1]): True})

    orders_customer_3 = orders_customer_2.sum(
        lambda x: {record({"c_count": x[0].c_count}): record({"custdist": (1.0) if (x[0].c_count != None) else (0.0)})})

    results = orders_customer_3.sum(lambda x: {x[0].concat(x[1]): True})

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q14(li, pa):
    promo = "PROMO"
    part_lineitem_probe_pre_ops = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19950901) * (x[0].l_shipdate < 19951001))) else (None))

    part_lineitem_build_nest_dict = pa.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    part_lineitem_0 = part_lineitem_probe_pre_ops.sum(
        lambda x: (part_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))

    part_lineitem_1 = part_lineitem_0.sum(lambda x: {x[0].concat(record({"A": (
    ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))) if (startsWith(x[0].p_type, promo)) else (0.0)})): x[1]})

    part_lineitem_2 = part_lineitem_1.sum(
        lambda x: {x[0].concat(record({"B": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    part_lineitem_3 = part_lineitem_2.sum(lambda x: record({"A_sum": x[0].A, "B_sum": x[0].B}))

    results = ((((part_lineitem_3.A_sum) * (100.0))) / (part_lineitem_3.B_sum))

    return results


#######

@sdql_compile({"li": lineitem_type, "su": supplier_type})
def q15(li, su):
    lineitem_0 = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19960101) * (x[0].l_shipdate < 19960401))) else (None))

    lineitem_1 = lineitem_0.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    lineitem_2 = lineitem_1.sum(
        lambda x: {record({"l_suppkey": x[0].l_suppkey}): record({"total_revenue": x[0].revenue})})

    lineitem_3 = lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})

    supplier_lineitem_probe_pre_ops = lineitem_3.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].total_revenue == 1614410.2928) else (None))

    supplier_lineitem_build_nest_dict = su.sum(lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    supplier_lineitem_0 = supplier_lineitem_probe_pre_ops.sum(
        lambda x: (supplier_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (supplier_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))

    results = supplier_lineitem_0.sum(lambda x: {record(
        {"s_suppkey": x[0].s_suppkey, "s_name": x[0].s_name, "s_address": x[0].s_address, "s_phone": x[0].s_phone,
         "total_revenue": x[0].total_revenue}): True})

    return results


#######

@sdql_compile({"ps": partsupp_type, "pa": part_type, "su": supplier_type})
def q16(ps, pa, su):
    brand45 = "Brand#45"
    mediumpolished = "MEDIUM POLISHED"
    customer = "Customer"
    complaints = "Complaints"
    part_0 = pa.sum(lambda x: ({x[0]: x[1]}) if ((
                (((x[0].p_brand != brand45) * (startsWith(x[0].p_type, mediumpolished) == False))) * ((((((((
                    (((((((x[0].p_size == 9) + (x[0].p_size == 36))) + (x[0].p_size == 49))) + (x[0].p_size == 14))) + (
                        x[0].p_size == 23))) + (x[0].p_size == 45))) + (x[0].p_size == 19))) + (x[
                                                                                                    0].p_size == 3))))) else (
        None))

    part_partsupp_build_pre_ops = part_0.sum(lambda x: {record(
        {"p_partkey": x[0].p_partkey, "p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size}): True})

    supplier_0 = su.sum(lambda x: ({x[0]: x[1]}) if (((firstIndex(x[0].s_comment, customer) != ((-1) * (1))) * (
                firstIndex(x[0].s_comment, complaints) > ((firstIndex(x[0].s_comment, customer)) + (7))))) else (None))

    supplier_partsupp_isin_pre_ops = supplier_0.sum(lambda x: {record({"s_suppkey": x[0].s_suppkey}): True})

    supplier_partsupp_isin_build_index = supplier_partsupp_isin_pre_ops.sum(lambda x: {x[0].s_suppkey: True})

    part_partsupp_probe_pre_ops = ps.sum(
        lambda x: ({x[0]: x[1]}) if (supplier_partsupp_isin_build_index[x[0].ps_suppkey] == None) else (None))

    part_partsupp_build_nest_dict = part_partsupp_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    part_partsupp_0 = part_partsupp_probe_pre_ops.sum(
        lambda x: (part_partsupp_build_nest_dict[x[0].ps_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_partsupp_build_nest_dict[x[0].ps_partkey] != None) else (None))

    part_partsupp_1 = part_partsupp_0.sum(lambda x: {
        record({"p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size}): record(
            {"supplier_cnt": sr_dict({x[0].ps_suppkey: True})})})

    results = part_partsupp_1.sum(lambda x: {record(
        {"p_brand": x[0].p_brand, "p_type": x[0].p_type, "p_size": x[0].p_size,
         "supplier_cnt": dictSize(x[1].supplier_cnt)}): True})

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q17(li, pa):
    brand23 = "Brand#23"
    medbox = "MED BOX"
    part_l1_build_pre_ops = pa.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].p_brand == brand23) * (x[0].p_container == medbox))) else (None))

    l1_0 = li.sum(lambda x: {record({"l_partkey": x[0].l_partkey}): record(
        {"sum_quant": x[0].l_quantity, "count_quant": (1.0) if (x[0].l_quantity != None) else (0.0)})})

    part_l1_probe_pre_ops = l1_0.sum(lambda x: {x[0].concat(x[1]): True})

    part_l1_build_nest_dict = part_l1_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    part_l1_lineitem_build_pre_ops = part_l1_probe_pre_ops.sum(
        lambda x: (part_l1_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_l1_build_nest_dict[x[0].l_partkey] != None) else (None))

    part_l1_lineitem_build_nest_dict = part_l1_lineitem_build_pre_ops.sum(
        lambda x: {x[0].l_partkey: sr_dict({x[0]: x[1]})})

    part_l1_lineitem_0 = li.sum(
        lambda x: (part_l1_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_l1_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))

    part_l1_lineitem_1 = part_l1_lineitem_0.sum(lambda x: {x[0].concat(record({"price": (x[0].l_extendedprice) if (
                x[0].l_quantity < ((0.2) * (((x[0].sum_quant) / (x[0].count_quant))))) else (0.0)})): x[1]})

    part_l1_lineitem_2 = part_l1_lineitem_1.sum(lambda x: record({"price_sum": x[0].price}))

    results = ((part_l1_lineitem_2.price_sum) / (7.0))

    return results


#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q18(li, cu, ord):
    lineitem_0 = li.sum(lambda x: {record({"l_orderkey": x[0].l_orderkey}): record({"sum_quantity": x[0].l_quantity})})

    lineitem_1 = lineitem_0.sum(lambda x: {x[0].concat(x[1]): True})

    lineitem_2 = lineitem_1.sum(lambda x: ({x[0]: x[1]}) if (x[0].sum_quantity > 300) else (None))

    lineitem_3 = lineitem_2.sum(lambda x: {x[0]: x[1]})

    lineitem_orders_isin_pre_ops = lineitem_3.sum(
        lambda x: {record({"l_orderkey": x[0].l_orderkey, "l_orderkey": x[0].l_orderkey}): True})

    lineitem_orders_isin_build_index = lineitem_orders_isin_pre_ops.sum(lambda x: {x[0].l_orderkey: True})

    customer_orders_probe_pre_ops = ord.sum(
        lambda x: ({x[0]: x[1]}) if (lineitem_orders_isin_build_index[x[0].o_orderkey] != None) else (None))

    customer_orders_build_nest_dict = cu.sum(lambda x: {x[0].c_custkey: sr_dict({x[0]: x[1]})})

    customer_orders_0 = customer_orders_probe_pre_ops.sum(
        lambda x: (customer_orders_build_nest_dict[x[0].o_custkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (customer_orders_build_nest_dict[x[0].o_custkey] != None) else (None))

    customer_orders_l1_build_pre_ops = customer_orders_0.sum(lambda x: {record(
        {"c_name": x[0].c_name, "c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey,
         "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice}): True})

    customer_orders_l1_build_nest_dict = customer_orders_l1_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    customer_orders_l1_0 = li.sum(
        lambda x: (customer_orders_l1_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (customer_orders_l1_build_nest_dict[x[0].l_orderkey] != None) else (None))

    customer_orders_l1_1 = customer_orders_l1_0.sum(lambda x: {record(
        {"c_name": x[0].c_name, "c_custkey": x[0].c_custkey, "o_orderkey": x[0].o_orderkey,
         "o_orderdate": x[0].o_orderdate, "o_totalprice": x[0].o_totalprice}): record(
        {"sum_quantity": x[0].l_quantity})})

    results = customer_orders_l1_1.sum(lambda x: {x[0].concat(x[1]): True})

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
    part_0 = pa.sum(lambda x: ({x[0]: x[1]}) if (((((((((((x[0].p_brand == brand12) * ((
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
                                                                 x[0].p_size >= 1))) * (x[0].p_size <= 15))))) else (
        None))

    part_lineitem_build_pre_ops = part_0.sum(
        lambda x: {record({"p_partkey": x[0].p_partkey, "p_brand": x[0].p_brand}): True})

    part_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (
    ((((x[0].l_shipmode == air) + (x[0].l_shipmode == airreg))) * (x[0].l_shipinstruct == deliverinperson))) else (
        None))

    part_lineitem_build_nest_dict = part_lineitem_build_pre_ops.sum(lambda x: {x[0].p_partkey: sr_dict({x[0]: x[1]})})

    part_lineitem_0 = part_lineitem_probe_pre_ops.sum(
        lambda x: (part_lineitem_build_nest_dict[x[0].l_partkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (part_lineitem_build_nest_dict[x[0].l_partkey] != None) else (None))

    part_lineitem_1 = part_lineitem_0.sum(lambda x: ({x[0]: x[1]}) if ((((
                (((x[0].p_brand == brand12) * (((x[0].l_quantity >= 1) * (x[0].l_quantity <= 11))))) + (
        ((x[0].p_brand == brand23) * (((x[0].l_quantity >= 10) * (x[0].l_quantity <= 20))))))) + ((
                (x[0].p_brand == brand34) * (((x[0].l_quantity >= 20) * (x[0].l_quantity <= 30))))))) else (None))

    part_lineitem_2 = part_lineitem_1.sum(
        lambda x: {x[0].concat(record({"revenue": ((x[0].l_extendedprice) * (((1.0) - (x[0].l_discount))))})): x[1]})

    part_lineitem_3 = part_lineitem_2.sum(lambda x: record({"revenue": x[0].revenue}))

    results = {part_lineitem_3: True}

    return results


#######

@sdql_compile({"su": supplier_type, "na": nation_type, "ps": partsupp_type, "pa": part_type, "li": lineitem_type})
def q20(su, na, ps, pa, li):
    canada = "CANADA"
    forest = "forest"
    nation_supplier_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == canada) else (None))

    lineitem_0 = li.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l_shipdate >= 19940101) * (x[0].l_shipdate < 19950101))) else (None))

    part_lineitem_isin_pre_ops = pa.sum(lambda x: ({x[0]: x[1]}) if (startsWith(x[0].p_name, forest)) else (None))

    part_lineitem_isin_build_index = part_lineitem_isin_pre_ops.sum(lambda x: {x[0].p_partkey: True})

    lineitem_1 = lineitem_0.sum(
        lambda x: ({x[0]: x[1]}) if (part_lineitem_isin_build_index[x[0].l_partkey] != None) else (None))

    supplier_lineitem_isin_build_index = su.sum(lambda x: {x[0].s_suppkey: True})

    lineitem_2 = lineitem_1.sum(
        lambda x: ({x[0]: x[1]}) if (supplier_lineitem_isin_build_index[x[0].l_suppkey] != None) else (None))

    lineitem_3 = lineitem_2.sum(lambda x: {
        record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): record({"sum_quantity": x[0].l_quantity})})

    lineitem_partsupp_build_pre_ops = lineitem_3.sum(lambda x: {x[0].concat(x[1]): True})

    lineitem_partsupp_build_nest_dict = lineitem_partsupp_build_pre_ops.sum(
        lambda x: {record({"l_partkey": x[0].l_partkey, "l_suppkey": x[0].l_suppkey}): sr_dict({x[0]: x[1]})})

    lineitem_partsupp_0 = ps.sum(lambda x: (
        lineitem_partsupp_build_nest_dict[record({"l_partkey": x[0].ps_partkey, "l_suppkey": x[0].ps_suppkey})].sum(
            lambda y: {x[0].concat(y[0]): True})
        ) if (lineitem_partsupp_build_nest_dict[
                  record({"l_partkey": x[0].ps_partkey, "l_suppkey": x[0].ps_suppkey})] != None) else (None))

    lineitem_partsupp_supplier_isin_pre_ops = lineitem_partsupp_0.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].ps_availqty > ((x[0].sum_quantity) * (0.5))) else (None))

    lineitem_partsupp_supplier_isin_build_index = lineitem_partsupp_supplier_isin_pre_ops.sum(
        lambda x: {x[0].l_suppkey: True})

    nation_supplier_probe_pre_ops = su.sum(
        lambda x: ({x[0]: x[1]}) if (lineitem_partsupp_supplier_isin_build_index[x[0].s_suppkey] != None) else (None))

    nation_supplier_build_nest_dict = nation_supplier_build_pre_ops.sum(
        lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    nation_supplier_0 = nation_supplier_probe_pre_ops.sum(
        lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    results = nation_supplier_0.sum(lambda x: {record({"s_name": x[0].s_name, "s_address": x[0].s_address}): True})

    return results


#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "na": nation_type})
def q21(su, li, ord, na):
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    orders_nation_supplier_l3_l2_lineitem_build_pre_ops = ord.sum(
        lambda x: ({x[0]: x[1]}) if (x[0].o_orderstatus == f) else (None))

    nation_supplier_build_pre_ops = na.sum(lambda x: ({x[0]: x[1]}) if (x[0].n_name == saudiarabia) else (None))

    nation_supplier_build_nest_dict = nation_supplier_build_pre_ops.sum(
        lambda x: {x[0].n_nationkey: sr_dict({x[0]: x[1]})})

    nation_supplier_l3_l2_lineitem_build_pre_ops = su.sum(
        lambda x: (nation_supplier_build_nest_dict[x[0].s_nationkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (nation_supplier_build_nest_dict[x[0].s_nationkey] != None) else (None))

    l3_0 = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))

    l3_1 = l3_0.sum(lambda x: {
        record({"l_orderkey": x[0].l_orderkey}): record({"l3_size": (1.0) if (x[0].l_suppkey != None) else (0.0)})})

    l3_2 = l3_1.sum(lambda x: {x[0].concat(x[1]): True})

    l3_l2_lineitem_build_pre_ops = l3_2.sum(
        lambda x: {record({"l_orderkey": x[0].l_orderkey, "l3_size": x[0].l3_size}): True})

    l2_0 = li.sum(lambda x: {
        record({"l_orderkey": x[0].l_orderkey}): record({"l2_size": (1.0) if (x[0].l_suppkey != None) else (0.0)})})

    l2_1 = l2_0.sum(lambda x: {x[0].concat(x[1]): True})

    l2_lineitem_build_pre_ops = l2_1.sum(
        lambda x: {record({"l_orderkey": x[0].l_orderkey, "l2_size": x[0].l2_size}): True})

    l2_lineitem_probe_pre_ops = li.sum(lambda x: ({x[0]: x[1]}) if (x[0].l_receiptdate > x[0].l_commitdate) else (None))

    l2_lineitem_build_nest_dict = l2_lineitem_build_pre_ops.sum(lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})

    l3_l2_lineitem_probe_pre_ops = l2_lineitem_probe_pre_ops.sum(
        lambda x: (l2_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (l2_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    l3_l2_lineitem_build_nest_dict = l3_l2_lineitem_build_pre_ops.sum(
        lambda x: {x[0].l_orderkey: sr_dict({x[0]: x[1]})})

    nation_supplier_l3_l2_lineitem_probe_pre_ops = l3_l2_lineitem_probe_pre_ops.sum(
        lambda x: (l3_l2_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
                   ) if (l3_l2_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    nation_supplier_l3_l2_lineitem_build_nest_dict = nation_supplier_l3_l2_lineitem_build_pre_ops.sum(
        lambda x: {x[0].s_suppkey: sr_dict({x[0]: x[1]})})

    orders_nation_supplier_l3_l2_lineitem_probe_pre_ops = nation_supplier_l3_l2_lineitem_probe_pre_ops.sum(lambda x: (
        nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_suppkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_suppkey] != None) else (None))

    orders_nation_supplier_l3_l2_lineitem_build_nest_dict = orders_nation_supplier_l3_l2_lineitem_build_pre_ops.sum(
        lambda x: {x[0].o_orderkey: sr_dict({x[0]: x[1]})})

    orders_nation_supplier_l3_l2_lineitem_0 = orders_nation_supplier_l3_l2_lineitem_probe_pre_ops.sum(lambda x: (
        orders_nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_orderkey].sum(lambda y: {x[0].concat(y[0]): True})
        ) if (orders_nation_supplier_l3_l2_lineitem_build_nest_dict[x[0].l_orderkey] != None) else (None))

    orders_nation_supplier_l3_l2_lineitem_1 = orders_nation_supplier_l3_l2_lineitem_0.sum(
        lambda x: ({x[0]: x[1]}) if (((x[0].l2_size > 1) * (x[0].l3_size == 1))) else (None))

    orders_nation_supplier_l3_l2_lineitem_2 = orders_nation_supplier_l3_l2_lineitem_1.sum(
        lambda x: {record({"s_name": x[0].s_name}): record({"numwait": (1.0) if (x[0].s_name != None) else (0.0)})})

    results = orders_nation_supplier_l3_l2_lineitem_2.sum(lambda x: {x[0].concat(x[1]): True})

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
    cu1_0 = cu.sum(lambda x: ({x[0]: x[1]}) if (((x[0].c_acctbal > 0.0) * ((((((((((
                (((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (
            startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone, v30)))) + (
                                                                                  startsWith(x[0].c_phone, v18)))) + (
                                                                                startsWith(x[0].c_phone,
                                                                                           v17)))))) else (None))

    cu1_1 = cu1_0.sum(lambda x: record({"sum_acctbal": x[0].c_acctbal, "count_acctbal": 1.0}))

    customer_0 = cu.sum(lambda x: ({x[0]: x[1]}) if ((
                (x[0].c_acctbal > ((cu1_1.sum_acctbal) / (cu1_1.count_acctbal))) * ((((((((((
                    (((startsWith(x[0].c_phone, v13)) + (startsWith(x[0].c_phone, v31)))) + (
                startsWith(x[0].c_phone, v23)))) + (startsWith(x[0].c_phone, v29)))) + (startsWith(x[0].c_phone,
                                                                                                   v30)))) + (
                                                                                           startsWith(x[0].c_phone,
                                                                                                      v18)))) + (
                                                                                         startsWith(x[0].c_phone,
                                                                                                    v17)))))) else (
        None))

    orders_customer_isin_build_index = ord.sum(lambda x: {x[0].o_custkey: True})

    customer_1 = customer_0.sum(
        lambda x: ({x[0]: x[1]}) if (orders_customer_isin_build_index[x[0].c_custkey] == None) else (None))

    customer_2 = customer_1.sum(lambda x: {x[0].concat(record({"cntrycode": substr(x[0].c_phone, 0, 1)})): x[1]})

    customer_3 = customer_2.sum(lambda x: {record({"cntrycode": x[0].cntrycode}): record({"numcust": 1.0, "totacctbal": x[0].c_acctbal})})

    results = customer_3.sum(lambda x: {x[0].concat(x[1]): True})

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