import gc

import duckdb
import pandas as pd

from pysdql.query.tpch.Qduck import *

from pysdql.query.tpch.const import (
    DATAPATH,
    LINEITEM_COLS,
    ORDERS_COLS,
    CUSTOMER_COLS,
    NATION_COLS,
    REGION_COLS,
    PART_COLS,
    SUPPLIER_COLS,
    PARTSUPP_COLS
)

from pysdql.query.tpch.template import *

from pysdql.query.util import compare_dataframe, pandas_to_df

# show all columns
pd.set_option('display.max_columns', None)
# suppress SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)


def check_duck(df1, df2):
    pd_duck_equal = compare_dataframe(df1, df2, for_duck=True)

    sep_line = '=' * 60

    if pd_duck_equal:
        print(sep_line)
        print(f'\033[32m Check Pandas with DuckDB: Pass \033[0m')
        print(sep_line)
    else:
        print(sep_line)
        print(f'\033[0m Check Pandas with DuckDB: Fail \033[0m')
        print(sep_line)


def q1():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q1(lineitem)

    duck_result = duck_conn.execute(duck_q1).df()

    check_duck(result, duck_result)

    del [[lineitem]]
    gc.collect()
    lineitem = pd.DataFrame()

    return result


def q2():
    duck_conn = duckdb.connect(database=':memory:')

    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    result = tpch_q2(part, supplier, partsupp, nation, region)

    duck_result = duck_conn.execute(duck_q2).df()

    check_duck(result, duck_result)

    del [[part, supplier, partsupp, nation, region]]
    gc.collect()
    part = pd.DataFrame()
    supplier = pd.DataFrame()
    partsupp = pd.DataFrame()
    nation = pd.DataFrame()
    region = pd.DataFrame()

    return result


def q3():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q3(lineitem, customer, orders)

    duck_result = duck_conn.execute(duck_q3).df()

    check_duck(result, duck_result)

    del [[lineitem, customer, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q4():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q4(orders, lineitem)

    duck_result = duck_conn.execute(duck_q4).df()

    check_duck(result, duck_result)

    del [[lineitem, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q5():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    result = tpch_q5(lineitem, customer, orders, region, nation, supplier)

    duck_result = duck_conn.execute(duck_q5).df()

    check_duck(result, duck_result)

    del [[lineitem, customer, orders, region, nation, supplier]]
    gc.collect()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    customer = pd.DataFrame()
    nation = pd.DataFrame()
    region = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q6():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = pandas_to_df(tpch_q6(lineitem))

    duck_result = duck_conn.execute(duck_q6).df()

    check_duck(result, duck_result)

    del [[lineitem]]
    gc.collect()
    lineitem = pd.DataFrame()

    return result


def q7():
    duck_conn = duckdb.connect(database=':memory:')

    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS,
                           parse_dates=["l_shipdate"])
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q7(supplier, lineitem, orders, customer, nation)

    duck_result = duck_conn.execute(duck_q7).df()

    check_duck(result, duck_result)

    del [[supplier, lineitem, orders, customer, nation]]
    gc.collect()
    supplier = pd.DataFrame()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    customer = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q8():
    duck_conn = duckdb.connect(database=':memory:')

    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    result = tpch_q8(part, supplier, lineitem, orders, customer, nation, region)

    duck_result = duck_conn.execute(duck_q8).df()

    check_duck(result, duck_result)

    del [[part, supplier, lineitem, orders, customer, nation, region]]
    gc.collect()
    part = pd.DataFrame()
    supplier = pd.DataFrame()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    customer = pd.DataFrame()
    nation = pd.DataFrame()
    region = pd.DataFrame()

    return result


def q9():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)

    result = tpch_q9(lineitem, orders, nation, supplier, part, partsupp)

    duck_result = duck_conn.execute(duck_q9).df()

    check_duck(result, duck_result)

    del [[lineitem, orders, nation, supplier, part, partsupp]]
    gc.collect()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    nation = pd.DataFrame()
    supplier = pd.DataFrame()
    part = pd.DataFrame()
    partsupp = pd.DataFrame()

    return result


def q10():
    duck_conn = duckdb.connect(database=':memory:')

    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q10(customer, orders, lineitem, nation)

    duck_result = duck_conn.execute(duck_q10).df()

    check_duck(result, duck_result)

    del [[customer, orders, lineitem, nation]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()
    lineitem = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q11():
    duck_conn = duckdb.connect(database=':memory:')

    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q11(partsupp, supplier, nation)

    duck_result = duck_conn.execute(duck_q11).df()

    check_duck(result, duck_result)

    del [[partsupp, supplier, nation]]
    gc.collect()
    partsupp = pd.DataFrame()
    supplier = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q12():
    duck_conn = duckdb.connect(database=':memory:')

    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q12(orders, lineitem)

    duck_result = duck_conn.execute(duck_q12).df()

    check_duck(result, duck_result)

    del [[orders, lineitem]]
    gc.collect()
    orders = pd.DataFrame()
    lineitem = pd.DataFrame()

    return result


def q13():
    duck_conn = duckdb.connect(database=':memory:')

    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q13(customer, orders)

    duck_result = duck_conn.execute(duck_q13).df()

    check_duck(result, duck_result)

    del [[customer, orders]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q14():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    result = pandas_to_df(tpch_q14(lineitem, part))

    duck_result = duck_conn.execute(duck_q14).df()

    check_duck(result, duck_result)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q15():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    result = tpch_q15(lineitem, supplier)

    duck_result = duck_conn.execute(duck_q15).df()

    check_duck(result, duck_result)

    del [[lineitem, supplier]]
    gc.collect()
    lineitem = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q16():
    duck_conn = duckdb.connect(database=':memory:')

    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    result = tpch_q16(partsupp, part, supplier)

    duck_result = duck_conn.execute(duck_q16).df()

    check_duck(result, duck_result)

    del [[partsupp, part, supplier]]
    gc.collect()
    partsupp = pd.DataFrame()
    part = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q17():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    result = tpch_q17(lineitem, part)

    duck_result = duck_conn.execute(duck_q17).df()

    check_duck(result, duck_result)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q18():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q18(lineitem, customer, orders)

    duck_result = duck_conn.execute(duck_q18).df()

    check_duck(result, duck_result)

    del [[lineitem, customer, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q19():
    duck_conn = duckdb.connect(database=':memory:')

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    result = tpch_q19(lineitem, part)

    duck_result = duck_conn.execute(duck_q19).df()

    check_duck(result, duck_result)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q20():
    duck_conn = duckdb.connect(database=':memory:')

    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q20(supplier, nation, partsupp, part, lineitem)

    duck_result = duck_conn.execute(duck_q20).df()

    check_duck(result, duck_result)

    del [[supplier, nation, partsupp, part, lineitem]]
    gc.collect()
    supplier = pd.DataFrame()
    nation = pd.DataFrame()
    partsupp = pd.DataFrame()
    part = pd.DataFrame()
    lineitem = pd.DataFrame()

    return result


def q21():
    duck_conn = duckdb.connect(database=':memory:')

    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q21(supplier, lineitem, orders, nation)

    duck_result = duck_conn.execute(duck_q21).df()

    check_duck(result, duck_result)

    del [[supplier, lineitem, orders, nation]]
    gc.collect()
    supplier = pd.DataFrame()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q22():
    duck_conn = duckdb.connect(database=':memory:')

    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q22(customer, orders)

    duck_result = duck_conn.execute(duck_q22).df()

    check_duck(result, duck_result)

    del [[customer, orders]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result
