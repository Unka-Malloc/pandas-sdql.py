"""
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date ':1'
    and l_shipdate < date ':1' + interval '1' year
    and l_discount between :2 - 0.01 and :2 + 0.01
    and l_quantity < :3
"""
import pandas

import pysdql

if __name__ == '__main__':
    var1 = '1993-01-01'
    var2 = '1994-01-01'  # var1 + 1 year
    discount = 0.07
    var3 = discount - 0.01
    var4 = discount + 0.01
    var5 = 25

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    r = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)
                 & (lineitem['l_discount'] > var3) & (lineitem['l_discount'] < var4)
                 & (lineitem['l_quantity'] < var5)]

    # r = lineitem[(lineitem['l_shipdate'] >= '1993-01-01') & (lineitem['l_shipdate'] < '1994-01-01')]  # 865
    # r = lineitem[(lineitem['l_discount'] >= 0.06) & (lineitem['l_discount'] <= 0.08)]
    # r = lineitem[lineitem['l_quantity'] < 25]

    r = r.aggr(revenue=(r['l_extendedprice'] * r['l_discount'], 'sum'))

    db_driver.run(r)
