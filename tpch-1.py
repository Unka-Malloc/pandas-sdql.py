"""
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
"""
from datetime import datetime, timedelta

import pysdql

if __name__ == '__main__':
    days = 76
    var1 = (datetime.strptime('1998-12-01', "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")

    db_driver = pysdql.db_driver(db_path=r'T:/sdql')

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)

    r = lineitem[(lineitem['l_shipdate'] <= var1)]

    r = r.groupby(['l_returnflag', 'l_linestatus'])
    r = r.aggr(sum_qty=(r['l_quantity'], 'sum'),
               sum_base_price=(r['l_extendedprice'], 'sum'),
               sum_disc_price=(r['l_extendedprice'] * (1 - r['l_discount']), 'sum'),
               sum_charge=(r['l_extendedprice'] * (1 - r['l_discount']) * (1 + r['l_tax']), 'sum'),
               avg_qty=(r['l_quantity'], 'avg'),
               avg_price=(r['l_extendedprice'], 'avg'),
               avg_disc=(r['l_discount'], 'avg'),
               count_order=('*', 'count'))

    db_driver.run(r)
