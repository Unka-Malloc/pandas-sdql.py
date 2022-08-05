"""
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' month
"""
import pysdql
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
# import numpy as np  # use numpy.select() must use together with pandas
import pysdql as pd  # get answer in pysdql
import pysdqlnp as np  # use pysdqlnp.select() must use together with pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = '1995-03-01'
    var2 = '1995-04-01'  # var1 + 1 month

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    part = pd.read_table(rf'{data_path}/part.tbl', sep='|', index_col=False, header=None, names=pysdql.PART_COLS)

    sub_l = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)]
    sub_l.columns.field = 'sub_l'

    r = sub_l.merge(part, left_on='l_partkey', right_on='p_partkey')

    r['value1'] = np.select(
        [
            r['p_type'].str.startswith('PROMO')
        ],
        [
            r['l_extendedprice'] * (1 - r['l_discount'])
        ],
        default=0)

    r['value2'] = r['l_extendedprice'] * (1 - r['l_discount'])

    r['promo_revenue'] = 100 * r['value1'].sum() / r['value2'].sum()

    r = r[['promo_revenue']].drop_duplicates(['promo_revenue'])

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-14').run(r).export().to()
