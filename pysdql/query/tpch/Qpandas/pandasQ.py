import gc
import time
import warnings

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

from pysdql.config import (
    is_verification_enabled,
    is_pandas_available,
    is_duckdb_available,
)

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    pd_start_time = time.time()

    result = tpch_q1(lineitem)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q1).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem]]
    gc.collect()
    lineitem = pd.DataFrame()

    return result


def q2():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    pd_start_time = time.time()

    result = tpch_q2(part, supplier, partsupp, nation, region)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q2).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    pd_start_time = time.time()

    result = tpch_q3(lineitem, customer, orders)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q3).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')
        check_duck(result, duck_result)

    del [[lineitem, customer, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q4():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    pd_start_time = time.time()

    result = tpch_q4(orders, lineitem)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q4).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q5():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    pd_start_time = time.time()

    result = tpch_q5(lineitem, customer, orders, region, nation, supplier)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q5).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    pd_start_time = time.time()

    result = pandas_to_df(tpch_q6(lineitem))

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q6).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem]]
    gc.collect()
    lineitem = pd.DataFrame()

    return result


def q7():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS,
                           parse_dates=["l_shipdate"])
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    pd_start_time = time.time()

    result = tpch_q7(supplier, lineitem, orders, customer, nation)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q7).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    pd_start_time = time.time()

    result = tpch_q8(part, supplier, lineitem, orders, customer, nation, region)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q8).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)

    pd_start_time = time.time()

    result = tpch_q9(lineitem, orders, nation, supplier, part, partsupp)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q9).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    pd_start_time = time.time()

    result = tpch_q10(customer, orders, lineitem, nation)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q10).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[customer, orders, lineitem, nation]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()
    lineitem = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q11():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    pd_start_time = time.time()

    result = tpch_q11(partsupp, supplier, nation)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q11).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[partsupp, supplier, nation]]
    gc.collect()
    partsupp = pd.DataFrame()
    supplier = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q12():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    pd_start_time = time.time()

    result = tpch_q12(orders, lineitem)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q12).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[orders, lineitem]]
    gc.collect()
    orders = pd.DataFrame()
    lineitem = pd.DataFrame()

    return result


def q13():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    pd_start_time = time.time()

    result = tpch_q13(customer, orders)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q13).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[customer, orders]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q14():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    pd_start_time = time.time()

    result = pandas_to_df(tpch_q14(lineitem, part))

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q14).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q15():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    pd_start_time = time.time()

    result = tpch_q15(lineitem, supplier)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q15).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem, supplier]]
    gc.collect()
    lineitem = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q16():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    pd_start_time = time.time()

    result = tpch_q16(partsupp, part, supplier)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q16).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[partsupp, part, supplier]]
    gc.collect()
    partsupp = pd.DataFrame()
    part = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q17():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    pd_start_time = time.time()

    result = pandas_to_df(tpch_q17(lineitem, part))

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q17).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q18():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    pd_start_time = time.time()

    result = tpch_q18(lineitem, customer, orders)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q18).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem, customer, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q19():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    pd_start_time = time.time()

    result = tpch_q19(lineitem, part)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q19).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q20():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    pd_start_time = time.time()

    result = tpch_q20(supplier, nation, partsupp, part, lineitem)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q20).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

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
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    pd_start_time = time.time()

    result = tpch_q21(supplier, lineitem, orders, nation)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')
        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q21).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[supplier, lineitem, orders, nation]]
    gc.collect()
    supplier = pd.DataFrame()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q22():
    if not is_pandas_available() & is_verification_enabled():
        warnings.warn("Warning: Pandas is not installed.")
        return

    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    pd_start_time = time.time()

    result = tpch_q22(customer, orders)

    pd_end_time = time.time()

    print(f'\033[36mPandas Execution Time: {pd_end_time - pd_start_time} s \033[0m')

    if is_duckdb_available() & is_verification_enabled():
        duck_conn = duckdb.connect(database=':memory:')

        duck_start_time = time.time()

        duck_result = duck_conn.execute(duck_q22).df()

        duck_end_time = time.time()

        print(f'\033[36mDuckDB Execution Time: {duck_end_time - duck_start_time} s \033[0m')

        check_duck(result, duck_result)

    del [[customer, orders]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result
