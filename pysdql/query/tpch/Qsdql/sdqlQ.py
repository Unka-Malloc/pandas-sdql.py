import os
import time

import pysdql

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
    sdqlpy_init,
)

from pysdql.query.tpch.template import *

Qfile_path = os.path.realpath(os.path.dirname(__file__))


def write_query(q: int, content: str):
    query_path = os.path.join(Qfile_path, f'Q{q}.py')

    old_lines = []

    with open(query_path, 'r') as f:
        for line in f:
            old_lines.append(line)

    first_index = old_lines.index('    # Insert\n')
    second_index = old_lines.index('    # Complete\n')

    first_lines = old_lines[:first_index + 1]
    second_lines = old_lines[second_index:]

    new_lines = first_lines + [f'{i}\n' for i in content.split('\n')] + second_lines

    with open(query_path, 'w') as f:
        for line in new_lines:
            f.write(line)


def q1(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()

    write_query(1, tpch_q1(lineitem).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")

    import pysdql.query.tpch.Qsdql.Q1 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q2(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    part = pysdql.DataFrame()
    supplier = pysdql.DataFrame()
    partsupp = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    region = pysdql.DataFrame()

    write_query(2, tpch_q2(part, supplier, partsupp, nation, region).to_sdqlir(optimize))

    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    partsupp_data = read_csv(rf'{DATAPATH}/partsupp.tbl', PARTSUPP_TYPE, "ps")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")
    region_data = read_csv(rf'{DATAPATH}/region.tbl', REGION_TYPE, "re")

    import pysdql.query.tpch.Qsdql.Q2 as Q

    query_start = time.time()

    sdql_result = Q.query(part_data, supplier_data, partsupp_data, nation_data, region_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q3(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    customer = pysdql.DataFrame()
    orders = pysdql.DataFrame()

    write_query(3, tpch_q3(lineitem, customer, orders).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")

    import pysdql.query.tpch.Qsdql.Q3 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, customer_data, orders_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q4(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    orders = pysdql.DataFrame()

    write_query(4, tpch_q4(orders, lineitem).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")

    import pysdql.query.tpch.Qsdql.Q4 as Q

    query_start = time.time()

    sdql_result = Q.query(orders_data, lineitem_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q5(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    customer = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    region = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    supplier = pysdql.DataFrame()

    write_query(5, tpch_q5(lineitem, customer, orders, region, nation, supplier).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    region_data = read_csv(rf'{DATAPATH}/region.tbl', REGION_TYPE, "re")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")

    import pysdql.query.tpch.Qsdql.Q5 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, customer_data, orders_data, region_data, nation_data, supplier_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q6(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()

    write_query(6, tpch_q6(lineitem).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")

    import pysdql.query.tpch.Qsdql.Q6 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q7(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    supplier = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    customer = pysdql.DataFrame()
    nation = pysdql.DataFrame()

    write_query(7, tpch_q7(supplier, lineitem, orders, customer, nation).to_sdqlir(optimize))

    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")

    import pysdql.query.tpch.Qsdql.Q7 as Q

    query_start = time.time()

    sdql_result = Q.query(supplier_data, lineitem_data, orders_data, customer_data, nation_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q8(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    part = pysdql.DataFrame()
    supplier = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    customer = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    region = pysdql.DataFrame()

    write_query(8, tpch_q8(part, supplier, lineitem, orders, customer, nation, region).to_sdqlir(optimize))

    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")
    region_data = read_csv(rf'{DATAPATH}/region.tbl', REGION_TYPE, "re")

    import pysdql.query.tpch.Qsdql.Q8 as Q

    query_start = time.time()

    sdql_result = Q.query(part_data, supplier_data, lineitem_data, orders_data, customer_data, nation_data, region_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q9(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    supplier = pysdql.DataFrame()
    part = pysdql.DataFrame()
    partsupp = pysdql.DataFrame()

    write_query(9, tpch_q9(lineitem, orders, nation, supplier, part, partsupp).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")
    partsupp_data = read_csv(rf'{DATAPATH}/partsupp.tbl', PARTSUPP_TYPE, "ps")

    import pysdql.query.tpch.Qsdql.Q9 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, orders_data, nation_data, supplier_data, part_data, partsupp_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q10(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    customer = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()
    nation = pysdql.DataFrame()

    write_query(10, tpch_q10(customer, orders, lineitem, nation).to_sdqlir(optimize))

    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")

    import pysdql.query.tpch.Qsdql.Q10 as Q

    query_start = time.time()

    sdql_result = Q.query(customer_data, orders_data, lineitem_data, nation_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q11(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    partsupp = pysdql.DataFrame()
    supplier = pysdql.DataFrame()
    nation = pysdql.DataFrame()

    write_query(11, tpch_q11(partsupp, supplier, nation).to_sdqlir(optimize))

    partsupp_data = read_csv(rf'{DATAPATH}/partsupp.tbl', PARTSUPP_TYPE, "ps")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")

    import pysdql.query.tpch.Qsdql.Q11 as Q

    query_start = time.time()

    sdql_result = Q.query(partsupp_data, supplier_data, nation_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q12(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    orders = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()

    write_query(12, tpch_q12(orders, lineitem).to_sdqlir(optimize))

    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")

    import pysdql.query.tpch.Qsdql.Q12 as Q

    query_start = time.time()

    sdql_result = Q.query(orders_data, lineitem_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q13(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    customer = pysdql.DataFrame()
    orders = pysdql.DataFrame()

    write_query(13, tpch_q13(customer, orders).to_sdqlir(optimize))

    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")

    import pysdql.query.tpch.Qsdql.Q13 as Q

    query_start = time.time()

    sdql_result = Q.query(customer_data, orders_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q14(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    part = pysdql.DataFrame()

    write_query(14, tpch_q14(lineitem, part).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")

    import pysdql.query.tpch.Qsdql.Q14 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, part_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q15(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    supplier = pysdql.DataFrame()

    write_query(15, tpch_q15(lineitem, supplier).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")

    import pysdql.query.tpch.Qsdql.Q15 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, supplier_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q16(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    partsupp = pysdql.DataFrame()
    part = pysdql.DataFrame()
    supplier = pysdql.DataFrame()

    write_query(16, tpch_q16(partsupp, part, supplier).to_sdqlir(optimize))

    partsupp_data = read_csv(rf'{DATAPATH}/partsupp.tbl', PARTSUPP_TYPE, "ps")
    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")
    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")

    import pysdql.query.tpch.Qsdql.Q16 as Q

    query_start = time.time()

    sdql_result = Q.query(partsupp_data, part_data, supplier_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q17(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    part = pysdql.DataFrame()

    write_query(17, tpch_q17(lineitem, part).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")

    import pysdql.query.tpch.Qsdql.Q17 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, part_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q18(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    customer = pysdql.DataFrame()
    orders = pysdql.DataFrame()

    write_query(18, tpch_q18(lineitem, customer, orders).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")

    import pysdql.query.tpch.Qsdql.Q18 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, customer_data, orders_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q19(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    lineitem = pysdql.DataFrame()
    part = pysdql.DataFrame()

    write_query(19, tpch_q19(lineitem, part).to_sdqlir(optimize))

    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")

    import pysdql.query.tpch.Qsdql.Q19 as Q

    query_start = time.time()

    sdql_result = Q.query(lineitem_data, part_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q20(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    supplier = pysdql.DataFrame()
    nation = pysdql.DataFrame()
    partsupp = pysdql.DataFrame()
    part = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()

    write_query(20, tpch_q20(supplier, nation, partsupp, part, lineitem).to_sdqlir(optimize))

    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")
    partsupp_data = read_csv(rf'{DATAPATH}/partsupp.tbl', PARTSUPP_TYPE, "ps")
    part_data = read_csv(rf'{DATAPATH}/part.tbl', PART_TYPE, "pa")
    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")

    import pysdql.query.tpch.Qsdql.Q20 as Q

    query_start = time.time()

    sdql_result = Q.query(supplier_data, nation_data, partsupp_data, part_data, lineitem_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q21(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    supplier = pysdql.DataFrame()
    lineitem = pysdql.DataFrame()
    orders = pysdql.DataFrame()
    nation = pysdql.DataFrame()

    write_query(21, tpch_q21(supplier, lineitem, orders, nation).to_sdqlir(optimize))

    supplier_data = read_csv(rf'{DATAPATH}/supplier.tbl', SUPPLIER_TYPE, "su")
    lineitem_data = read_csv(rf'{DATAPATH}/lineitem.tbl', LINEITEM_TYPE, "li")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")
    nation_data = read_csv(rf'{DATAPATH}/nation.tbl', NATION_TYPE, "na")

    import pysdql.query.tpch.Qsdql.Q21 as Q

    query_start = time.time()

    sdql_result = Q.query(supplier_data, lineitem_data, orders_data, nation_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result


def q22(execution_mode=0, threads_count=1, optimize=True):
    sdqlpy_init(execution_mode, threads_count)

    customer = pysdql.DataFrame()
    orders = pysdql.DataFrame()

    write_query(22, tpch_q22(customer, orders).to_sdqlir(optimize))

    customer_data = read_csv(rf'{DATAPATH}/customer.tbl', CUSTOMER_TYPE, "cu")
    orders_data = read_csv(rf'{DATAPATH}/orders.tbl', ORDERS_TYPE, "ord")

    import pysdql.query.tpch.Qsdql.Q22 as Q

    query_start = time.time()

    sdql_result = Q.query(customer_data, orders_data)

    query_end = time.time()

    print(f'\033[36m SDQL Execution Time: {query_end - query_start} s \033[0m')

    return sdql_result
