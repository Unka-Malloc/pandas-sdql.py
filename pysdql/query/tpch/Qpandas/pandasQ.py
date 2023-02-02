import gc

import pandas as pd

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

# show all columns
pd.set_option('display.max_columns', None)
# suppress SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)


def q1():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q1(lineitem)

    del [[lineitem]]
    gc.collect()
    lineitem = pd.DataFrame()

    return result


def q3():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q3(lineitem, customer, orders)

    del [[lineitem, customer, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q4():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q4(orders, lineitem)

    del [[lineitem, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q5():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    result = tpch_q5(lineitem, customer, orders, region, nation, supplier)

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
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q6(lineitem)

    del [[lineitem]]
    gc.collect()
    lineitem = pd.DataFrame()

    return result


def q7():
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS,
                           parse_dates=["l_shipdate"])
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q7(supplier, lineitem, orders, customer, nation)

    del [[supplier, lineitem, orders, customer, nation]]
    gc.collect()
    supplier = pd.DataFrame()
    lineitem = pd.DataFrame()
    orders = pd.DataFrame()
    customer = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q8():
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    region = pd.read_csv(rf'{DATAPATH}/region.tbl', sep='|', index_col=False, header=None, names=REGION_COLS)

    result = tpch_q8(part, supplier, lineitem, orders, customer, nation, region)

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
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS,
                         parse_dates=['o_orderdate'])
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)

    result = tpch_q9(lineitem, orders, nation, supplier, part, partsupp)

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
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q10(customer, orders, lineitem, nation)

    del [[customer, orders, lineitem, nation]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()
    lineitem = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q11():
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)

    result = tpch_q11(partsupp, supplier, nation)

    del [[partsupp, supplier, nation]]
    gc.collect()
    partsupp = pd.DataFrame()
    supplier = pd.DataFrame()
    nation = pd.DataFrame()

    return result


def q12():
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q12(orders, lineitem)

    del [[orders, lineitem]]
    gc.collect()
    orders = pd.DataFrame()
    lineitem = pd.DataFrame()

    return result


def q13():
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q13(customer, orders)

    del [[customer, orders]]
    gc.collect()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q14():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    result = tpch_q14(lineitem, part)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q15():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    result = tpch_q15(lineitem, supplier)

    del [[lineitem, supplier]]
    gc.collect()
    lineitem = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q16():
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)

    result = tpch_q16(partsupp, part, supplier)

    del [[partsupp, part, supplier]]
    gc.collect()
    partsupp = pd.DataFrame()
    part = pd.DataFrame()
    supplier = pd.DataFrame()

    return result


def q17():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    result = tpch_q17(lineitem, part)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q18():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    customer = pd.read_csv(rf'{DATAPATH}/customer.tbl', sep='|', index_col=False, header=None, names=CUSTOMER_COLS)
    orders = pd.read_csv(rf'{DATAPATH}/orders.tbl', sep='|', index_col=False, header=None, names=ORDERS_COLS)

    result = tpch_q18(lineitem, customer, orders)

    del [[lineitem, customer, orders]]
    gc.collect()
    lineitem = pd.DataFrame()
    customer = pd.DataFrame()
    orders = pd.DataFrame()

    return result


def q19():
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)

    result = tpch_q19(lineitem, part)

    del [[lineitem, part]]
    gc.collect()
    lineitem = pd.DataFrame()
    part = pd.DataFrame()

    return result


def q20():
    supplier = pd.read_csv(rf'{DATAPATH}/supplier.tbl', sep='|', index_col=False, header=None, names=SUPPLIER_COLS)
    nation = pd.read_csv(rf'{DATAPATH}/nation.tbl', sep='|', index_col=False, header=None, names=NATION_COLS)
    partsupp = pd.read_csv(rf'{DATAPATH}/partsupp.tbl', sep='|', index_col=False, header=None, names=PARTSUPP_COLS)
    part = pd.read_csv(rf'{DATAPATH}/part.tbl', sep='|', index_col=False, header=None, names=PART_COLS)
    lineitem = pd.read_csv(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    result = tpch_q20(supplier, nation, partsupp, part, lineitem)

    del [[supplier, nation, partsupp, part, lineitem]]
    gc.collect()
    supplier = pd.DataFrame()
    nation = pd.DataFrame()
    partsupp = pd.DataFrame()
    part = pd.DataFrame()
    lineitem = pd.DataFrame()

    return result
