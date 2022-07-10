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
    lineitem = pysdql.Relation(name='lineitem', cols=pysdql.LINEITEM_COLS)

    lineitem = lineitem[(lineitem.l_shipdate >= ':1')
                        & (lineitem.l_shipdate < ':1 + 1 year')
                        & (lineitem.l_discount > ':2 - 0.01')
                        & (lineitem.l_discount < ':2 + 0.01')
                        & (lineitem.l_quantity > ':3')]

    lineitem.aggr(revenue=(lineitem['l_extendedprice'] * lineitem['l_discount'], 'sum'))
