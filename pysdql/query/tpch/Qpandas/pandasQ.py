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

# show all columns
pd.set_option('display.max_columns', None)
# suppress SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

def q1():
    lineitem = pd.read_table(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    li_filt = lineitem[(lineitem['l_shipdate'] <= "1998-09-02")]
    li_filt["disc_price"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])
    li_filt["charge"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount']) * (1.0 + li_filt['l_tax'])

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"], as_index=False) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count"))

    return result

def q6():
    lineitem = pd.read_table(rf'{DATAPATH}/lineitem.tbl', sep='|', index_col=False, header=None, names=LINEITEM_COLS)

    li_filt = lineitem[
        (lineitem.l_shipdate >= "1994-01-01") &
        (lineitem.l_shipdate < "1995-01-01") &
        (lineitem.l_discount >= 0.05) &
        (lineitem.l_discount <= 0.07) &
        (lineitem.l_quantity < 24)
        ]

    li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

    result = li_filt.agg({'revenue': 'sum'})

    return result


