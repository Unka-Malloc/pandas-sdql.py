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

import pysdql

if __name__ == '__main__':
    db_driver = pysdql.driver(db_path=r'T:/sdql')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    r = lineitem[(lineitem.l_shipdate >= 19960301)
                 & (lineitem.l_shipdate < 19970301)
                 & (lineitem.l_discount > 0.09)
                 & (lineitem.l_discount < 1.01)
                 & (lineitem.l_quantity > 20)]

    r = r.aggr(revenue=(lineitem['l_extendedprice'] * lineitem['l_discount'], 'sum'))

    db_driver.run(r)
