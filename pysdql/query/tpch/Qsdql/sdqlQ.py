from pysdql.query.tpch.const import (
    DATAPATH,
    LINEITEM_TYPE,
    ORDERS_TYPE,
    CUSTOMER_TYPE,
    NATION_TYPE,
    REGION_TYPE,
    PART_TYPE,
    SUPPLIER_TYPE,
    PARTSUPP_TYPE
)

from pysdql.extlib.sdqlpy.sdql_lib import (
    read_csv,
    sdqlpy_init
)

from pysdql.query.tpch.Qsdql import (
    Q1,
    Q6
)

def q1(execution_mode=0, threads_count=1):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")

    return Q1.query(lineitem)

def q6(execution_mode=0, threads_count=1):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")

    return Q6.query(lineitem)




