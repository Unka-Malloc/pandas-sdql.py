# primary key: P_PARTKEY
# PART_COLS = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE',
#              'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT']
PART_COLS = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']
PART_LOAD = 'let part = load[{<p_partkey: int, p_name: string, p_mfgr: string, p_brand: string, p_type: string, p_size: int, p_container: string, p_retailprice: real, p_comment: string> -> int}]("datasets/tuned/part.tbl")'

# Primary Key: S_SUPPKEY
# SUPPLIER_COLS = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE',
#                  'S_ACCTBAL', 'S_COMMENT']
SUPPLIER_COLS = ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
SUPPLIER_LOAD = 'let supplier = load[{<s_suppkey: int, s_name: string, s_address: string, s_nationkey: int, s_phone: string, s_acctbal: real, s_comment: string> -> int}]("datasets/tuned/supplier.tbl")'

# Primary Key: PS_PARTKEY, PS_SUPPKEY
# PARTSUPP_COLS = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
PARTSUPP_COLS = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']
PARTSUPP_LOAD = 'let partsupp = load[{<ps_partkey: int, ps_suppkey: int, ps_availqty: int, ps_supplycost: real, ps_comment: string> -> int}]("datasets/tuned/partsupp.tbl")'

# Primary Key: C_CUSTKEY
# CUSTOMER_COLS = ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE',
                 # 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT']

CUSTOMER_COLS = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']
CUSTOMER_LOAD = 'let customer = load[{<c_custkey: int, c_name: string, c_address: string, c_nationkey: int, c_phone: string, c_acctbal: real, c_mktsegment: string, c_comment: string> -> int}]("datasets/tuned/customer.tbl")'

# Primary Key: O_ORDERKEY
# ORDERS_COLS = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE',
#                'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']

ORDERS_COLS = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']
ORDERS_LOAD = 'let orders = load[{<o_orderkey: int, o_custkey: int, o_orderstatus: string, o_totalprice: real, o_orderdate: date, o_orderpriority: string, o_clerk: string, o_shippriority: int, o_comment: string> -> int}]("datasets/tuned/orders.tbl")'

# Primary Key: L_ORDERKEY, L_LINENUMBER
# LINEITEM_COLS = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
#                  'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS',
#                  'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE',
#                  'L_COMMENT']

LINEITEM_COLS = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
LINEITEM_LOAD = 'let lineitem = load[{<l_orderkey: int, l_partkey: int, l_suppkey: int, l_linenumber: int, l_quantity: int, l_extendedprice: real, l_discount: real, l_tax: real, l_returnflag: string, l_linestatus: string, l_shipdate: date, l_commitdate: date, l_receiptdate: date, l_shipinstruct: string, l_shipmode: string, l_comment: string> -> int}]("datasets/tuned/lineitem.tbl")'

# Primary Key: N_NATIONKEY
# NATION_COLS = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']

NATION_COLS = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
NATION_LOAD = 'let nation = load[{<n_nationkey: int, n_name: string, n_regionkey: int, n_comment: string> -> int}]("datasets/tuned/nation.tbl")'

# Primary Key: R_REGIONKEY
# REGION_COLS = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']

REGION_COLS = ['r_regionkey', 'r_name', 'r_comment']
REGION_LOAD = 'let region = load[{<r_regionkey: int, r_name: string, r_comment: string> -> int}]("datasets/tuned/region.tbl")'
