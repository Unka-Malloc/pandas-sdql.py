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
show_results = True

## Number of iterations for benchmarking each query (must be >=2)
iterations = 2

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
    lineitem_aggr = li.sum(lambda x_lineitem: ({
        record({"l_returnflag": x_lineitem[0].l_returnflag, "l_linestatus": x_lineitem[0].l_linestatus}): record(
            {"sum_qty": x_lineitem[0].l_quantity, "sum_base_price": x_lineitem[0].l_extendedprice,
             "sum_disc_price": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount)))),
             "sum_charge": ((((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) * (
             ((1.0) + (x_lineitem[0].l_tax)))), "avg_disc_sum_for_mean": x_lineitem[0].l_discount,
             "count_order": 1.0})}) if (x_lineitem[0].l_shipdate <= 19980902) else (None))

    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record(
        {"l_returnflag": x_lineitem_aggr[0].l_returnflag, "l_linestatus": x_lineitem_aggr[0].l_linestatus,
         "sum_qty": x_lineitem_aggr[1].sum_qty, "sum_base_price": x_lineitem_aggr[1].sum_base_price,
         "sum_disc_price": x_lineitem_aggr[1].sum_disc_price, "sum_charge": x_lineitem_aggr[1].sum_charge,
         "avg_qty": ((x_lineitem_aggr[1].sum_qty) / (x_lineitem_aggr[1].count_order)),
         "avg_price": ((x_lineitem_aggr[1].sum_base_price) / (x_lineitem_aggr[1].count_order)),
         "avg_disc": ((x_lineitem_aggr[1].avg_disc_sum_for_mean) / (x_lineitem_aggr[1].count_order)),
         "count_order": x_lineitem_aggr[1].count_order}): True})

    return results


######

@sdql_compile({"pa": part_type, "su": supplier_type, "ps": partsupp_type, "na": nation_type, "re": region_type})
def q2(pa, su, ps, na, re):
    europe = "EUROPE"
    brass = "BRASS"
    region_part = re.sum(
        lambda x_region: ({x_region[0].r_regionkey: True}) if (x_region[0].r_name == europe) else (None))

    region_nation = na.sum(lambda x_nation: (
    {x_nation[0].n_nationkey: record({"n_nationkey": x_nation[0].n_nationkey, "n_name": x_nation[0].n_name})}) if (
                region_part[x_nation[0].n_regionkey] != None) else (None))

    region_nation_supplier = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: record(
        {"s_suppkey": x_supplier[0].s_suppkey, "s_acctbal": x_supplier[0].s_acctbal, "s_name": x_supplier[0].s_name,
         "n_name": region_nation[x_supplier[0].s_nationkey].n_name, "s_address": x_supplier[0].s_address,
         "s_phone": x_supplier[0].s_phone, "s_comment": x_supplier[0].s_comment})}) if (
                region_nation[x_supplier[0].s_nationkey] != None) else (None))

    region_nation_supplier_partsupp = ps.sum(lambda x_ps1: ({x_ps1[0].ps_partkey: record(
        {"min_supplycost": x_ps1[0].ps_supplycost, "ps_partkey": x_ps1[0].ps_partkey, "ps_suppkey": x_ps1[0].ps_suppkey,
         "s_suppkey": x_ps1[0].ps_suppkey})}) if (region_nation_supplier[x_ps1[0].ps_suppkey] != None) else (None))

    part_part = pa.sum(lambda x_part: (
    {x_part[0].p_partkey: record({"p_partkey": x_part[0].p_partkey, "p_mfgr": x_part[0].p_mfgr})}) if (
    ((endsWith(x_part[0].p_type, brass)) * (x_part[0].p_size == 15))) else (None))

    results = ps.sum(lambda x_partsupp: (((({record(
        {"s_acctbal": region_nation_supplier[x_partsupp[0].ps_suppkey].s_acctbal,
         "s_name": region_nation_supplier[x_partsupp[0].ps_suppkey].s_name,
         "n_name": region_nation_supplier[x_partsupp[0].ps_suppkey].n_name,
         "p_partkey": part_part[x_partsupp[0].ps_partkey].p_partkey,
         "p_mfgr": part_part[x_partsupp[0].ps_partkey].p_mfgr,
         "s_address": region_nation_supplier[x_partsupp[0].ps_suppkey].s_address,
         "s_phone": region_nation_supplier[x_partsupp[0].ps_suppkey].s_phone,
         "s_comment": region_nation_supplier[x_partsupp[0].ps_suppkey].s_comment}): True}) if (
                x_partsupp[0].ps_supplycost == region_nation_supplier_partsupp[
            x_partsupp[0].ps_partkey].min_supplycost) else (None)) if (
                region_nation_supplier[x_partsupp[0].ps_suppkey] != None) else (None)) if (
                region_nation_supplier_partsupp[x_partsupp[0].ps_partkey] != None) else (None)) if (
                part_part[x_partsupp[0].ps_partkey] != None) else (None))

    return results


######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q3(li, cu, ord):
    building = "BUILDING"
    customer_part = cu.sum(
        lambda x_customer: ({x_customer[0].c_custkey: True}) if (x_customer[0].c_mktsegment == building) else (None))

    customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record(
        {"o_orderdate": x_orders[0].o_orderdate, "o_shippriority": x_orders[0].o_shippriority})}) if (
                customer_part[x_orders[0].o_custkey] != None) else (None)) if (
                x_orders[0].o_orderdate < 19950315) else (None))

    lineitem_aggr = li.sum(lambda x_lineitem: (({record(
        {"l_orderkey": x_lineitem[0].l_orderkey, "o_orderdate": customer_orders[x_lineitem[0].l_orderkey].o_orderdate,
         "o_shippriority": customer_orders[x_lineitem[0].l_orderkey].o_shippriority}): (
                (x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))}) if (
                customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (
                x_lineitem[0].l_shipdate > 19950315) else (None))

    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record(
        {"l_orderkey": x_lineitem_aggr[0].l_orderkey, "o_orderdate": x_lineitem_aggr[0].o_orderdate,
         "o_shippriority": x_lineitem_aggr[0].o_shippriority, "revenue": x_lineitem_aggr[1]}): True})

    return results


#####

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q4(ord, li):
    lineitem_part = li.sum(lambda x_lineitem: ({x_lineitem[0].l_orderkey: True}) if (
                x_lineitem[0].l_commitdate < x_lineitem[0].l_receiptdate) else (None))

    orders_aggr = ord.sum(lambda x_orders: (
        ({x_orders[0].o_orderpriority: 1.0}) if (lineitem_part[x_orders[0].o_orderkey] != None) else (None)) if (
    ((x_orders[0].o_orderdate >= 19930701) * (x_orders[0].o_orderdate < 19931001))) else (None))

    results = orders_aggr.sum(
        lambda x_orders_aggr: {record({"o_orderpriority": x_orders_aggr[0], "order_count": x_orders_aggr[1]}): True})

    return results


#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type, "re": region_type, "na": nation_type,
               "su": supplier_type})
def q5(li, cu, ord, re, na, su):
    asia = "ASIA"
    region_part = re.sum(lambda x_region: ({x_region[0].r_regionkey: True}) if (x_region[0].r_name == asia) else (None))

    region_nation = na.sum(lambda x_nation: ({x_nation[0].n_nationkey: record({"n_name": x_nation[0].n_name})}) if (
                region_part[x_nation[0].n_regionkey] != None) else (None))

    region_nation_customer = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: record(
        {"c_nationkey": x_customer[0].c_nationkey, "n_name": region_nation[x_customer[0].c_nationkey].n_name})}) if (
                region_nation[x_customer[0].c_nationkey] != None) else (None))

    region_nation_customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record(
        {"c_nationkey": region_nation_customer[x_orders[0].o_custkey].c_nationkey,
         "n_name": region_nation_customer[x_orders[0].o_custkey].n_name})}) if (
                region_nation_customer[x_orders[0].o_custkey] != None) else (None)) if (
    ((x_orders[0].o_orderdate >= 19940101) * (x_orders[0].o_orderdate < 19950101))) else (None))

    supplier_part = su.sum(lambda x_supplier: {
        record({"s_suppkey": x_supplier[0].s_suppkey, "s_nationkey": x_supplier[0].s_nationkey}): True})

    supplier_region_nation_customer_orders_lineitem = li.sum(lambda x_lineitem: (({
        region_nation_customer_orders[x_lineitem[0].l_orderkey].n_name: record(
            {"revenue": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})}) if (
                supplier_part[record({"l_suppkey": x_lineitem[0].l_suppkey,
                                      "c_nationkey": region_nation_customer_orders[
                                          x_lineitem[0].l_orderkey].c_nationkey})] != None) else (None)) if (
                region_nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None))

    results = supplier_region_nation_customer_orders_lineitem.sum(
        lambda x_supplier_region_nation_customer_orders_lineitem: {record(
            {"n_name": x_supplier_region_nation_customer_orders_lineitem[0],
             "revenue": x_supplier_region_nation_customer_orders_lineitem[1].revenue}): True})

    return results


#######

@sdql_compile({"li": lineitem_type})
def q6(li):
    lineitem_aggr = li.sum(lambda x_lineitem: (((x_lineitem[0].l_extendedprice) * (x_lineitem[0].l_discount))) if ((((((
                (((x_lineitem[0].l_shipdate >= 19940101) * (x_lineitem[0].l_shipdate < 19950101))) * (
                    x_lineitem[0].l_discount >= 0.05))) * (x_lineitem[0].l_discount <= 0.07))) * (x_lineitem[
                                                                                                      0].l_quantity < 24))) else (
        0.0))

    results = {record({"revenue": lineitem_aggr}): True}

    return results


#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type, "na": nation_type})
def q7(su, li, ord, cu, na):
    france = "FRANCE"
    germany = "GERMANY"
    n1_part = na.sum(lambda x_n1: ({x_n1[0].n_nationkey: record({"n_name": x_n1[0].n_name})}) if (
    ((x_n1[0].n_name == france) + (x_n1[0].n_name == germany))) else (None))

    nation_supplier = su.sum(lambda x_supplier: (
    {x_supplier[0].s_suppkey: record({"n1_name": n1_part[x_supplier[0].s_nationkey].n_name})}) if (
                n1_part[x_supplier[0].s_nationkey] != None) else (None))

    n2_part = na.sum(lambda x_n2: ({x_n2[0].n_nationkey: record({"n_name": x_n2[0].n_name})}) if (
    ((x_n2[0].n_name == france) + (x_n2[0].n_name == germany))) else (None))

    nation_customer = cu.sum(lambda x_customer: (
    {x_customer[0].c_custkey: record({"n_name": n2_part[x_customer[0].c_nationkey].n_name})}) if (
                n2_part[x_customer[0].c_nationkey] != None) else (None))

    nation_customer_orders = ord.sum(lambda x_orders: (
    {x_orders[0].o_orderkey: record({"n2_name": nation_customer[x_orders[0].o_custkey].n_name})}) if (
                nation_customer[x_orders[0].o_custkey] != None) else (None))

    nation_supplier_nation_customer_orders_lineitem = li.sum(lambda x_lineitem: (((({record(
        {"supp_nation": nation_supplier[x_lineitem[0].l_suppkey].n1_name,
         "cust_nation": nation_customer_orders[x_lineitem[0].l_orderkey].n2_name,
         "l_year": extractYear(x_lineitem[0].l_shipdate)}): record(
        {"revenue": ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})}) if ((((
                (nation_supplier[x_lineitem[0].l_suppkey].n1_name == france) * (
                    nation_customer_orders[x_lineitem[0].l_orderkey].n2_name == germany))) + ((
                (nation_supplier[x_lineitem[0].l_suppkey].n1_name == germany) * (
                    nation_customer_orders[x_lineitem[0].l_orderkey].n2_name == france))))) else (None)) if (
                nation_supplier[x_lineitem[0].l_suppkey] != None) else (None)) if (
                nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (
    ((x_lineitem[0].l_shipdate >= 19950101) * (x_lineitem[0].l_shipdate <= 19961231))) else (None))

    results = nation_supplier_nation_customer_orders_lineitem.sum(
        lambda x_nation_supplier_nation_customer_orders_lineitem: {record(
            {"supp_nation": x_nation_supplier_nation_customer_orders_lineitem[0].supp_nation,
             "cust_nation": x_nation_supplier_nation_customer_orders_lineitem[0].cust_nation,
             "l_year": x_nation_supplier_nation_customer_orders_lineitem[0].l_year,
             "revenue": x_nation_supplier_nation_customer_orders_lineitem[1].revenue}): True})

    return results


######

@sdql_compile({"pa": part_type, "su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type,
               "na": nation_type, "re": region_type})
def q8(pa, su, li, ord, cu, na, re):
    economyanodizedsteel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"
    n2_part = na.sum(lambda x_n2: {x_n2[0].n_nationkey: record(
        {"n2_comment": x_n2[0].n_comment, "n2_name": x_n2[0].n_name, "n2_nationkey": x_n2[0].n_nationkey,
         "n2_regionkey": x_n2[0].n_regionkey})})

    supplier_part = su.sum(
        lambda x_supplier: {x_supplier[0].s_suppkey: record({"s_nationkey": x_supplier[0].s_nationkey})})

    part_part = pa.sum(
        lambda x_part: ({x_part[0].p_partkey: True}) if (x_part[0].p_type == economyanodizedsteel) else (None))

    region_part = re.sum(
        lambda x_region: ({x_region[0].r_regionkey: True}) if (x_region[0].r_name == america) else (None))

    region_nation = na.sum(lambda x_n1: ({x_n1[0].n_nationkey: record({"n1_nationkey": x_n1[0].n_nationkey})}) if (
                region_part[x_n1[0].n_regionkey] != None) else (None))

    region_nation_customer = cu.sum(lambda x_customer: ({x_customer[0].c_custkey: True}) if (
                region_nation[x_customer[0].c_nationkey] != None) else (None))

    region_nation_customer_orders = ord.sum(lambda x_orders: (
        ({x_orders[0].o_orderkey: record({"o_orderdate": x_orders[0].o_orderdate})}) if (
                    region_nation_customer[x_orders[0].o_custkey] != None) else (None)) if (
    ((x_orders[0].o_orderdate >= 19950101) * (x_orders[0].o_orderdate <= 19961231))) else (None))

    nation_supplier_part_region_nation_customer_orders_lineitem = li.sum(lambda x_lineitem: (((({
        extractYear(region_nation_customer_orders[x_lineitem[0].l_orderkey].o_orderdate): record({"A": (
        ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) if (
                    n2_part[supplier_part[x_lineitem[0].l_suppkey].s_nationkey].n2_name == brazil) else (0.0), "B": (
                    (x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})}) if (
                n2_part[supplier_part[x_lineitem[0].l_suppkey].s_nationkey] != None) else (None)) if (
                supplier_part[x_lineitem[0].l_suppkey] != None) else (None)) if (
                part_part[x_lineitem[0].l_partkey] != None) else (None)) if (
                region_nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None))

    results = nation_supplier_part_region_nation_customer_orders_lineitem.sum(
        lambda x_nation_supplier_part_region_nation_customer_orders_lineitem: {record(
            {"o_year": x_nation_supplier_part_region_nation_customer_orders_lineitem[0], "mkt_share": (
                        (x_nation_supplier_part_region_nation_customer_orders_lineitem[1].A) / (
                    x_nation_supplier_part_region_nation_customer_orders_lineitem[1].B))}): True})

    return results


######

@sdql_compile({"li": lineitem_type, "ord": order_type, "na": nation_type, "su": supplier_type, "pa": part_type,
               "ps": partsupp_type})
def q9(li, ord, na, su, pa, ps):
    green = "green"
    nation_part = na.sum(lambda x_nation: {x_nation[0].n_nationkey: record({"n_name": x_nation[0].n_name})})

    nation_supplier = su.sum(lambda x_supplier: (
    {x_supplier[0].s_suppkey: record({"n_name": nation_part[x_supplier[0].s_nationkey].n_name})}) if (
                nation_part[x_supplier[0].s_nationkey] != None) else (None))

    part_part = pa.sum(
        lambda x_part: ({x_part[0].p_partkey: True}) if (firstIndex(x_part[0].p_name, green) != ((-1) * (1))) else (
            None))

    nation_supplier_part_partsupp = ps.sum(lambda x_partsupp: (({
        record({"ps_suppkey": x_partsupp[0].ps_suppkey, "ps_partkey": x_partsupp[0].ps_partkey}): record(
            {"n_name": nation_supplier[x_partsupp[0].ps_suppkey].n_name, "ps_partkey": x_partsupp[0].ps_partkey,
             "ps_suppkey": x_partsupp[0].ps_suppkey, "ps_supplycost": x_partsupp[0].ps_supplycost})}) if (
                nation_supplier[x_partsupp[0].ps_suppkey] != None) else (None)) if (
                part_part[x_partsupp[0].ps_partkey] != None) else (None))

    orders_part = ord.sum(lambda x_orders: {x_orders[0].o_orderkey: record({"o_orderdate": x_orders[0].o_orderdate})})

    nation_supplier_part_partsupp_orders_lineitem = li.sum(lambda x_lineitem: (({record({"nation":
                                                                                             nation_supplier_part_partsupp[
                                                                                                 record({"l_suppkey":
                                                                                                             x_lineitem[
                                                                                                                 0].l_suppkey,
                                                                                                         "l_partkey":
                                                                                                             x_lineitem[
                                                                                                                 0].l_partkey})].n_name,
                                                                                         "o_year": extractYear(
                                                                                             orders_part[x_lineitem[
                                                                                                 0].l_orderkey].o_orderdate)}): record(
        {"sum_profit": ((((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) - (((
                                                                                                             nation_supplier_part_partsupp[
                                                                                                                 record(
                                                                                                                     {
                                                                                                                         "l_suppkey":
                                                                                                                             x_lineitem[
                                                                                                                                 0].l_suppkey,
                                                                                                                         "l_partkey":
                                                                                                                             x_lineitem[
                                                                                                                                 0].l_partkey})].ps_supplycost) * (
                                                                                                             x_lineitem[
                                                                                                                 0].l_quantity))))})}) if (
                nation_supplier_part_partsupp[record(
                    {"l_suppkey": x_lineitem[0].l_suppkey, "l_partkey": x_lineitem[0].l_partkey})] != None) else (
        None)) if (orders_part[x_lineitem[0].l_orderkey] != None) else (None))

    results = nation_supplier_part_partsupp_orders_lineitem.sum(
        lambda x_nation_supplier_part_partsupp_orders_lineitem: {record(
            {"nation": x_nation_supplier_part_partsupp_orders_lineitem[0].nation,
             "o_year": x_nation_supplier_part_partsupp_orders_lineitem[0].o_year,
             "sum_profit": x_nation_supplier_part_partsupp_orders_lineitem[1].sum_profit}): True})

    return results


######

@sdql_compile({"cu": customer_type, "ord": order_type, "li": lineitem_type, "na": nation_type})
def q10(cu, ord, li, na):
    r = "R"
    nation_part = na.sum(lambda x_nation: {x_nation[0].n_nationkey: record({"n_name": x_nation[0].n_name})})

    customer_part = cu.sum(lambda x_customer: {x_customer[0].c_custkey: record(
        {"c_custkey": x_customer[0].c_custkey, "c_name": x_customer[0].c_name, "c_acctbal": x_customer[0].c_acctbal,
         "c_phone": x_customer[0].c_phone, "c_address": x_customer[0].c_address, "c_comment": x_customer[0].c_comment,
         "c_nationkey": x_customer[0].c_nationkey})})

    nation_customer_orders = ord.sum(lambda x_orders: (((({x_orders[0].o_orderkey: record(
        {"o_orderkey": x_orders[0].o_orderkey, "c_custkey": x_orders[0].o_custkey,
         "c_name": customer_part[x_orders[0].o_custkey].c_name,
         "c_acctbal": customer_part[x_orders[0].o_custkey].c_acctbal,
         "c_phone": customer_part[x_orders[0].o_custkey].c_phone,
         "n_name": nation_part[customer_part[x_orders[0].o_custkey].c_nationkey].n_name,
         "c_address": customer_part[x_orders[0].o_custkey].c_address,
         "c_comment": customer_part[x_orders[0].o_custkey].c_comment})}) if (
                customer_part[x_orders[0].o_custkey] != None) else (None)) if (
                nation_part[customer_part[x_orders[0].o_custkey].c_nationkey] != None) else (None)) if (
                customer_part[x_orders[0].o_custkey] != None) else (None)) if (
    ((x_orders[0].o_orderdate >= 19931001) * (x_orders[0].o_orderdate < 19940101))) else (None))

    lineitem_aggr = li.sum(lambda x_lineitem: (({record(
        {"c_custkey": nation_customer_orders[x_lineitem[0].l_orderkey].c_custkey,
         "c_name": nation_customer_orders[x_lineitem[0].l_orderkey].c_name,
         "c_acctbal": nation_customer_orders[x_lineitem[0].l_orderkey].c_acctbal,
         "c_phone": nation_customer_orders[x_lineitem[0].l_orderkey].c_phone,
         "n_name": nation_customer_orders[x_lineitem[0].l_orderkey].n_name,
         "c_address": nation_customer_orders[x_lineitem[0].l_orderkey].c_address,
         "c_comment": nation_customer_orders[x_lineitem[0].l_orderkey].c_comment}): (
                (x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))}) if (
                nation_customer_orders[x_lineitem[0].l_orderkey] != None) else (None)) if (
                x_lineitem[0].l_returnflag == r) else (None))

    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record(
        {"c_custkey": x_lineitem_aggr[0].c_custkey, "c_name": x_lineitem_aggr[0].c_name,
         "c_acctbal": x_lineitem_aggr[0].c_acctbal, "c_phone": x_lineitem_aggr[0].c_phone,
         "n_name": x_lineitem_aggr[0].n_name, "c_address": x_lineitem_aggr[0].c_address,
         "c_comment": x_lineitem_aggr[0].c_comment, "revenue": x_lineitem_aggr[1]}): True})

    return results


#######

@sdql_compile({"ps": partsupp_type, "su": supplier_type, "na": nation_type})
def q11(ps, su, na):
    germany = "GERMANY"
    nation_part = na.sum(
        lambda x_nation: ({x_nation[0].n_nationkey: True}) if (x_nation[0].n_name == germany) else (None))

    nation_supplier = su.sum(
        lambda x_supplier: ({x_supplier[0].s_suppkey: True}) if (nation_part[x_supplier[0].s_nationkey] != None) else (
            None))

    partsupp_aggr = ps.sum(lambda x_partsupp: (record(
        {"filt_val": ((((x_partsupp[0].ps_supplycost) * (x_partsupp[0].ps_availqty))) * (0.0001)), "filt_agg": sr_dict(
            {x_partsupp[0].ps_partkey: ((x_partsupp[0].ps_supplycost) * (x_partsupp[0].ps_availqty))})})) if (
                nation_supplier[x_partsupp[0].ps_suppkey] != None) else (None))

    results = partsupp_aggr.filt_agg.sum(
        lambda x_partsupp_aggr: ({record({"ps_partkey": x_partsupp_aggr[0], "value": x_partsupp_aggr[1]}): True}) if (
                    x_partsupp_aggr[1] > partsupp_aggr.filt_val) else (None))

    return results


######

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q12(ord, li):
    mail = "MAIL"
    ship = "SHIP"
    urgent1 = "1-URGENT"
    high2 = "2-HIGH"
    orders_part = ord.sum(
        lambda x_orders: {x_orders[0].o_orderkey: record({"o_orderpriority": x_orders[0].o_orderpriority})})

    lineitem_aggr = li.sum(lambda x_lineitem: (({x_lineitem[0].l_shipmode: record({"high_line_count": (1) if ((
                (orders_part[x_lineitem[0].l_orderkey].o_orderpriority == urgent1) + (
                    orders_part[x_lineitem[0].l_orderkey].o_orderpriority == high2))) else (0),
                                                                                   "low_line_count": (1) if (((
                                                                                                                          orders_part[
                                                                                                                              x_lineitem[
                                                                                                                                  0].l_orderkey].o_orderpriority != urgent1) * (
                                                                                                                          orders_part[
                                                                                                                              x_lineitem[
                                                                                                                                  0].l_orderkey].o_orderpriority != high2))) else (
                                                                                       0)})}) if (
                orders_part[x_lineitem[0].l_orderkey] != None) else (None)) if ((((((((
                (((x_lineitem[0].l_shipmode == ship) + (x_lineitem[0].l_shipmode == mail))) * (
                    x_lineitem[0].l_commitdate < x_lineitem[0].l_receiptdate))) * (x_lineitem[0].l_shipdate <
                                                                                   x_lineitem[0].l_commitdate))) * (
                                                                                               x_lineitem[
                                                                                                   0].l_receiptdate >= 19940101))) * (
                                                                                             x_lineitem[
                                                                                                 0].l_receiptdate < 19950101))) else (
        None))

    results = lineitem_aggr.sum(lambda x_lineitem_aggr: {record(
        {"l_shipmode": x_lineitem_aggr[0], "high_line_count": x_lineitem_aggr[1].high_line_count,
         "low_line_count": x_lineitem_aggr[1].low_line_count}): True})

    return results


######

@sdql_compile({"cu": customer_type, "ord": order_type})
def q13(cu, ord):
    special = "special"
    requests = "requests"
    orders_part = ord.sum(lambda x_orders: ({x_orders[0].o_custkey: record({"c_count": 1.0})}) if (((firstIndex(
        x_orders[0].o_comment, special) != -1) * (firstIndex(x_orders[0].o_comment, requests) > (
                (firstIndex(x_orders[0].o_comment, special)) + (6)))) == False) else (None))

    customer_aggr = cu.sum(lambda x_customer: {record({"c_count": (orders_part[x_customer[0].c_custkey].c_count) if (
                orders_part[x_customer[0].c_custkey] != None) else (0.0)}): 1.0})

    results = customer_aggr.sum(
        lambda x_customer_aggr: {record({"c_count": x_customer_aggr[0].c_count, "custdist": x_customer_aggr[1]}): True})

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q14(li, pa):
    promo = "PROMO"
    part_part = pa.sum(
        lambda x_part: ({x_part[0].p_partkey: True}) if (startsWith(x_part[0].p_type, promo)) else (None))

    part_lineitem = li.sum(lambda x_lineitem: (record({"A": (
    ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) if (
                part_part[x_lineitem[0].l_partkey] != None) else (0.0), "B": (
                (x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))})) if (
    ((x_lineitem[0].l_shipdate >= 19950901) * (x_lineitem[0].l_shipdate < 19951001))) else (None))

    results = ((((part_lineitem.A) * (100.0))) / (part_lineitem.B))

    return results


#######

@sdql_compile({"li": lineitem_type, "su": supplier_type})
def q15(li, su):
    lineitem_aggr = li.sum(lambda x_lineitem: (
    {x_lineitem[0].l_suppkey: ((x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))}) if (
    ((x_lineitem[0].l_shipdate >= 19960101) * (x_lineitem[0].l_shipdate < 19960401))) else (None))

    supplier_part = su.sum(lambda x_supplier: {x_supplier[0].s_suppkey: record(
        {"s_suppkey": x_supplier[0].s_suppkey, "s_name": x_supplier[0].s_name, "s_address": x_supplier[0].s_address,
         "s_phone": x_supplier[0].s_phone})})

    results = lineitem_aggr.sum(lambda x_lineitem_aggr: (({record(
        {"s_suppkey": x_lineitem_aggr[0], "s_name": supplier_part[x_lineitem_aggr[0]].s_name,
         "s_address": supplier_part[x_lineitem_aggr[0]].s_address, "s_phone": supplier_part[x_lineitem_aggr[0]].s_phone,
         "total_revenue": x_lineitem_aggr[1]}): True}) if (x_lineitem_aggr[1] == 1772627.2087) else (None)) if (
                supplier_part[x_lineitem_aggr[0]] != None) else (None))

    return results


#######

@sdql_compile({"ps": partsupp_type, "pa": part_type, "su": supplier_type})
def q16(ps, pa, su):
    brand45 = "Brand#45"
    mediumpolished = "MEDIUM POLISHED"
    customer = "Customer"
    complaints = "Complaints"
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: record(
        {"p_partkey": x_part[0].p_partkey, "p_brand": x_part[0].p_brand, "p_type": x_part[0].p_type,
         "p_size": x_part[0].p_size})}) if ((
                (((x_part[0].p_brand != brand45) * (startsWith(x_part[0].p_type, mediumpolished) == False))) * (((((((((
                                                                                                                       (
                                                                                                                                   (
                                                                                                                                   (
                                                                                                                                               (
                                                                                                                                               (
                                                                                                                                                           (
                                                                                                                                                                       x_part[
                                                                                                                                                                           0].p_size == 9) + (
                                                                                                                                                                       x_part[
                                                                                                                                                                           0].p_size == 36))) + (
                                                                                                                                                           x_part[
                                                                                                                                                               0].p_size == 49))) + (
                                                                                                                                               x_part[
                                                                                                                                                   0].p_size == 14))) + (
                                                                                                                                   x_part[
                                                                                                                                       0].p_size == 23))) + (
                                                                                                                                 x_part[
                                                                                                                                     0].p_size == 45))) + (
                                                                                                                               x_part[
                                                                                                                                   0].p_size == 19))) + (
                                                                                                                             x_part[
                                                                                                                                 0].p_size == 3))))) else (
        None))

    supplier_part = su.sum(lambda x_supplier: ({x_supplier[0].s_suppkey: True}) if ((
                (firstIndex(x_supplier[0].s_comment, customer) != ((-1) * (1))) * (
                    firstIndex(x_supplier[0].s_comment, complaints) > (
                        (firstIndex(x_supplier[0].s_comment, customer)) + (7))))) else (None))

    partsupp_aggr = ps.sum(lambda x_partsupp: (({record(
        {"p_brand": part_part[x_partsupp[0].ps_partkey].p_brand, "p_type": part_part[x_partsupp[0].ps_partkey].p_type,
         "p_size": part_part[x_partsupp[0].ps_partkey].p_size}): 1.0}) if (
                supplier_part[x_partsupp[0].ps_suppkey] == None) else (None)) if (
                part_part[x_partsupp[0].ps_partkey] != None) else (None))

    results = partsupp_aggr.sum(lambda x_partsupp_aggr: {record(
        {"p_brand": x_partsupp_aggr[0].p_brand, "p_type": x_partsupp_aggr[0].p_type,
         "p_size": x_partsupp_aggr[0].p_size, "supplier_cnt": x_partsupp_aggr[1]}): True})

    return results


#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q17(li, pa):
    brand23 = "Brand#23"
    medbox = "MED BOX"
    part_part = pa.sum(lambda x_part: ({x_part[0].p_partkey: True}) if (
    ((x_part[0].p_brand == brand23) * (x_part[0].p_container == medbox))) else (None))

    part_l1 = li.sum(
        lambda x_l1: ({x_l1[0].l_partkey: record({"count_quant": 1.0, "sum_quant": x_l1[0].l_quantity})}) if (
                    part_part[x_l1[0].l_partkey] != None) else (None))

    part_l1_lineitem = li.sum(lambda x_lineitem: record({"price": ((x_lineitem[0].l_extendedprice) if (
                x_lineitem[0].l_quantity < ((0.2) * (
        ((part_l1[x_lineitem[0].l_partkey].sum_quant) / (part_l1[x_lineitem[0].l_partkey].count_quant))))) else (
        0.0) if (part_l1[x_lineitem[0].l_partkey] != None) else (0.0))}))

    results = ((part_l1_lineitem.price) / (7.0))

    return results


#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q18(li, cu, ord):
    customer_part = cu.sum(lambda x_customer: {
        x_customer[0].c_custkey: record({"c_custkey": x_customer[0].c_custkey, "c_name": x_customer[0].c_name})})

    lineitem_aggr = li.sum(lambda x_lineitem: {x_lineitem[0].l_orderkey: x_lineitem[0].l_quantity})

    lineitem_part = lineitem_aggr.sum(
        lambda x_lineitem_aggr: ({x_lineitem_aggr[0]: True}) if (x_lineitem_aggr[1] > 300) else (None))

    customer_orders = ord.sum(lambda x_orders: (({x_orders[0].o_orderkey: record(
        {"c_custkey": x_orders[0].o_custkey, "c_name": customer_part[x_orders[0].o_custkey].c_name,
         "o_orderdate": x_orders[0].o_orderdate, "o_orderkey": x_orders[0].o_orderkey,
         "o_totalprice": x_orders[0].o_totalprice})}) if (customer_part[x_orders[0].o_custkey] != None) else (
        None)) if (lineitem_part[x_orders[0].o_orderkey] != None) else (None))

    l1_aggr = li.sum(lambda x_l1: ({record({"c_name": customer_orders[x_l1[0].l_orderkey].c_name,
                                            "c_custkey": customer_orders[x_l1[0].l_orderkey].c_custkey,
                                            "o_orderkey": x_l1[0].l_orderkey,
                                            "o_orderdate": customer_orders[x_l1[0].l_orderkey].o_orderdate,
                                            "o_totalprice": customer_orders[x_l1[0].l_orderkey].o_totalprice}): x_l1[
        0].l_quantity}) if (customer_orders[x_l1[0].l_orderkey] != None) else (None))

    results = l1_aggr.sum(lambda x_l1_aggr: {record(
        {"c_name": x_l1_aggr[0].c_name, "c_custkey": x_l1_aggr[0].c_custkey, "o_orderkey": x_l1_aggr[0].o_orderkey,
         "o_orderdate": x_l1_aggr[0].o_orderdate, "o_totalprice": x_l1_aggr[0].o_totalprice,
         "sum_quantity": x_l1_aggr[1]}): True})

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
    part_part = pa.sum(lambda x_part: (
    {x_part[0].p_partkey: record({"p_partkey": x_part[0].p_partkey, "p_brand": x_part[0].p_brand})}) if ((((((((((
                (x_part[0].p_brand == brand12) * ((((
                    (((x_part[0].p_container == smpkg) + (x_part[0].p_container == smpack))) + (
                        x_part[0].p_container == smcase))) + (x_part[0].p_container == smbox))))) * (x_part[
                                                                                                         0].p_size >= 1))) * (
                                                                                                                          x_part[
                                                                                                                              0].p_size <= 5))) + (
                                                                                                            ((((((
                                                                                                                             x_part[
                                                                                                                                 0].p_brand == brand23) * (
                                                                                                                 ((((((
                                                                                                                                  x_part[
                                                                                                                                      0].p_container == medpack) + (
                                                                                                                                  x_part[
                                                                                                                                      0].p_container == medpkg))) + (
                                                                                                                                x_part[
                                                                                                                                    0].p_container == medbag))) + (
                                                                                                                              x_part[
                                                                                                                                  0].p_container == medbox))))) * (
                                                                                                                           x_part[
                                                                                                                               0].p_size >= 1))) * (
                                                                                                                         x_part[
                                                                                                                             0].p_size <= 10))))) + (
                                                                                                          ((((((x_part[
                                                                                                                    0].p_brand == brand34) * (
                                                                                                               ((((((
                                                                                                                                x_part[
                                                                                                                                    0].p_container == lgpkg) + (
                                                                                                                                x_part[
                                                                                                                                    0].p_container == lgpack))) + (
                                                                                                                              x_part[
                                                                                                                                  0].p_container == lgcase))) + (
                                                                                                                            x_part[
                                                                                                                                0].p_container == lgbox))))) * (
                                                                                                                         x_part[
                                                                                                                             0].p_size >= 1))) * (
                                                                                                                       x_part[
                                                                                                                           0].p_size <= 15))))) else (
        None))

    lineitem_aggr = li.sum(lambda x_lineitem: ((((
                (x_lineitem[0].l_extendedprice) * (((1.0) - (x_lineitem[0].l_discount))))) if ((((((
                (part_part[x_lineitem[0].l_partkey].p_brand == brand12) * (
        ((x_lineitem[0].l_quantity >= 1) * (x_lineitem[0].l_quantity <= 11))))) + ((
                (part_part[x_lineitem[0].l_partkey].p_brand == brand23) * (
        ((x_lineitem[0].l_quantity >= 10) * (x_lineitem[0].l_quantity <= 20))))))) + ((
                (part_part[x_lineitem[0].l_partkey].p_brand == brand34) * (
        ((x_lineitem[0].l_quantity >= 20) * (x_lineitem[0].l_quantity <= 30))))))) else (0.0)) if (
                part_part[x_lineitem[0].l_partkey] != None) else (0.0)) if ((
                (((x_lineitem[0].l_shipmode == air) + (x_lineitem[0].l_shipmode == airreg))) * (
                    x_lineitem[0].l_shipinstruct == deliverinperson))) else (0.0))

    results = {record({"revenue": lineitem_aggr}): True}

    return results


#######

@sdql_compile({"su": supplier_type, "na": nation_type, "ps": partsupp_type, "pa": part_type, "li": lineitem_type})
def q20(su, na, ps, pa, li):
    canada = "CANADA"
    forest = "forest"
    nation_part = na.sum(
        lambda x_nation: ({x_nation[0].n_nationkey: True}) if (x_nation[0].n_name == canada) else (None))

    part_part = pa.sum(
        lambda x_part: ({x_part[0].p_partkey: True}) if (startsWith(x_part[0].p_name, forest)) else (None))

    supplier_part = su.sum(lambda x_supplier: {x_supplier[0].s_suppkey: True})

    lineitem_part = li.sum(lambda x_lineitem: ((({
        record({"l_partkey": x_lineitem[0].l_partkey, "l_suppkey": x_lineitem[0].l_suppkey}): record(
            {"sum_quantity": x_lineitem[0].l_quantity})}) if (part_part[x_lineitem[0].l_partkey] != None) else (
        None)) if (supplier_part[x_lineitem[0].l_suppkey] != None) else (None)) if (
    ((x_lineitem[0].l_shipdate >= 19940101) * (x_lineitem[0].l_shipdate < 19950101))) else (None))

    lineitem_partsupp = ps.sum(lambda x_partsupp: (({x_partsupp[0].ps_suppkey: True}) if (x_partsupp[0].ps_availqty > ((
                                                                                                                           lineitem_part[
                                                                                                                               record(
                                                                                                                                   {
                                                                                                                                       "ps_partkey":
                                                                                                                                           x_partsupp[
                                                                                                                                               0].ps_partkey,
                                                                                                                                       "ps_suppkey":
                                                                                                                                           x_partsupp[
                                                                                                                                               0].ps_suppkey})].sum_quantity) * (
                                                                                                                           0.5))) else (
        None)) if (lineitem_part[record(
        {"ps_partkey": x_partsupp[0].ps_partkey, "ps_suppkey": x_partsupp[0].ps_suppkey})] != None) else (None))

    results = su.sum(lambda x_supplier: (
        ({record({"s_name": x_supplier[0].s_name, "s_address": x_supplier[0].s_address}): True}) if (
                    nation_part[x_supplier[0].s_nationkey] != None) else (None)) if (
                lineitem_partsupp[x_supplier[0].s_suppkey] != None) else (None))

    return results


#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "na": nation_type})
def q21(su, li, ord, na):
    f = "F"
    saudiarabia = "SAUDI ARABIA"
    orders_part = ord.sum(
        lambda x_orders: ({x_orders[0].o_orderkey: True}) if (x_orders[0].o_orderstatus == f) else (None))

    nation_part = na.sum(
        lambda x_nation: ({x_nation[0].n_nationkey: True}) if (x_nation[0].n_name == saudiarabia) else (None))

    nation_supplier = su.sum(
        lambda x_supplier: ({x_supplier[0].s_suppkey: record({"s_name": x_supplier[0].s_name})}) if (
                nation_part[x_supplier[0].s_nationkey] != None) else (None))

    l3_part = li.sum(lambda x_l3: ({x_l3[0].l_orderkey: record({"l3_size": 1})}) if (
            x_l3[0].l_receiptdate > x_l3[0].l_commitdate) else (None))

    l2_part = li.sum(lambda x_l2: {x_l2[0].l_orderkey: record({"l2_size": 1})})

    orders_nation_supplier_l3_l2_lineitem = li.sum(lambda x_lineitem: (((((({
        nation_supplier[x_lineitem[0].l_suppkey].s_name: record({"numwait": 1.0})}) if (
        ((l2_part[x_lineitem[0].l_orderkey].l2_size > 1) * (l3_part[x_lineitem[0].l_orderkey].l3_size == 1))) else (
        None)) if (orders_part[x_lineitem[0].l_orderkey] != None) else (None)) if (
            nation_supplier[x_lineitem[0].l_suppkey] != None) else (None)) if (
            l3_part[x_lineitem[0].l_orderkey] != None) else (None)) if (
            l2_part[x_lineitem[0].l_orderkey] != None) else (None)) if (
            x_lineitem[0].l_receiptdate > x_lineitem[0].l_commitdate) else (None))

    results = orders_nation_supplier_l3_l2_lineitem.sum(lambda x_orders_nation_supplier_l3_l2_lineitem: {record(
        {"s_name": x_orders_nation_supplier_l3_l2_lineitem[0],
         "numwait": x_orders_nation_supplier_l3_l2_lineitem[1].numwait}): True})

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
    cu1_aggr = cu.sum(lambda x_cu1: (record({"sum_acctbal": x_cu1[0].c_acctbal, "count_acctbal": 1.0})) if ((
                (x_cu1[0].c_acctbal > 0.0) * ((((((((((
                    (((startsWith(x_cu1[0].c_phone, v13)) + (startsWith(x_cu1[0].c_phone, v31)))) + (
                startsWith(x_cu1[0].c_phone, v23)))) + (startsWith(x_cu1[0].c_phone, v29)))) + (
                                                       startsWith(x_cu1[0].c_phone, v30)))) + (
                                                     startsWith(x_cu1[0].c_phone, v18)))) + (
                                                   startsWith(x_cu1[0].c_phone, v17)))))) else (None))

    count_acctbal = cu1_aggr.count_acctbal
    sum_acctbal = cu1_aggr.sum_acctbal
    orders_part = ord.sum(lambda x_orders: {x_orders[0].o_custkey: True})

    customer_aggr = cu.sum(lambda x_customer: (
        ({substr(x_customer[0].c_phone, 0, 1): record({"numcust": 1.0, "totacctbal": x_customer[0].c_acctbal})}) if (
                    orders_part[x_customer[0].c_custkey] == None) else (None)) if ((
                (x_customer[0].c_acctbal > ((sum_acctbal) / (count_acctbal))) * ((((((((((
                    (((startsWith(x_customer[0].c_phone, v13)) + (startsWith(x_customer[0].c_phone, v31)))) + (
                startsWith(x_customer[0].c_phone, v23)))) + (startsWith(x_customer[0].c_phone, v29)))) + (startsWith(
            x_customer[0].c_phone, v30)))) + (startsWith(x_customer[0].c_phone, v18)))) + (
                                                                                      startsWith(x_customer[0].c_phone,
                                                                                                 v17)))))) else (None))

    results = customer_aggr.sum(lambda x_customer_aggr: {record(
        {"cntrycode": x_customer_aggr[0], "numcust": x_customer_aggr[1].numcust,
         "totacctbal": x_customer_aggr[1].totacctbal}): True})

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
