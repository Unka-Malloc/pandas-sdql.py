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

# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
import pysdql as pd  # get answer in pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    days = 76
    var1 = str((datetime.strptime('1998-12-01', "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d"))

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)

    r = lineitem[lineitem['l_shipdate'] <= var1]

    r['disc_price'] = r['l_extendedprice'] * (1 - r['l_discount'])
    r['charge'] = r['l_extendedprice'] * (1 - r['l_discount']) * (1 + r['l_tax'])

    r = r.groupby(['l_returnflag', 'l_linestatus'], as_index=False) \
        .agg(sum_qty=('l_quantity', 'sum'),
             sum_base_price=('l_extendedprice', 'sum'),
             sum_disc_price=('disc_price', 'sum'),
             sum_charge=('charge', 'sum'),
             avg_qty=('l_quantity', 'mean'),
             avg_price=('l_extendedprice', 'mean'),
             avg_disc=('l_discount', 'mean'),
             count_order=('l_quantity', 'count'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-1').run(r).export().to()
