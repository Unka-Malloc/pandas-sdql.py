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
    var1 = '1997-01-01'
    var2 = '1998-01-01'  # var1 + 1 year
    discount = 0.06
    var3 = round(discount - 0.01, 2)
    var4 = round(discount + 0.01, 2)
    var5 = 24

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)

    r = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)
                 & (lineitem['l_discount'] > var3) & (lineitem['l_discount'] < var4)
                 & (lineitem['l_quantity'] < var5)]

    r = r.aggregate(revenue=(r['l_extendedprice'] * r['l_discount'], 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-6').run(r).export().to()
