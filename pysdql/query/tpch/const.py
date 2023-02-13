from pysdql.extlib.sdqlpy.sdql_lib import (
    record,
    string,
    date
)

# There should not be a '/' at the end.
# Path/should/be/like/this
# Not/like/this/

# DATAPATH should contain all 8 tbl files

DATAPATH = r'V:/Datasets/TPCH/100M'

LINEITEM_COLS = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount',
                 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct',
                 'l_shipmode', 'l_comment']
ORDERS_COLS = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk',
               'o_shippriority', 'o_comment']
CUSTOMER_COLS = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']
NATION_COLS = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
REGION_COLS = ['r_regionkey', 'r_name', 'r_comment']
PART_COLS = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']
SUPPLIER_COLS = ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
PARTSUPP_COLS = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']

LINEITEM_TYPE = {record(
    {"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float,
     "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1),
     "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date,
     "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
CUSTOMER_TYPE = {record(
    {"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15),
     "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
ORDERS_TYPE = {record(
    {"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date,
     "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79),
     "o_NA": string(1)}): bool}
NATION_TYPE = {record({"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152),
                       "n_NA": string(1)}): bool}
REGION_TYPE = {
    record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}
PART_TYPE = {record(
    {"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25),
     "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23),
     "p_NA": string(1)}): bool}
SUPPLIER_TYPE = {record(
    {"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15),
     "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}
PARTSUPP_TYPE = {record(
    {"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199),
     "ps_NA": string(1)}): bool}
