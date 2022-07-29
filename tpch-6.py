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
# Try replace pysdql with pandas to get result in pandas!
# import pandas as pd  # get answer in pandas
# import numpy as np  # for numpy.select(), must use together with pandas
import pysdql as pd  # get answer in pysdql
# import pysdqlnp as np  # for pysdqlnp.select(), must use together with pysdql

# display all columns
pd.set_option('display.max_columns', None)
# display all rows
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    data_path = 'T:/UG4-Proj/datasets'
    sdql_database_path = r'T:/sdql'

    var1 = '1997-01-01'
    var2 = '1998-01-01'  # var1 + 1 year
    discount = 0.06
    var3 = round(discount - 0.01, 2)
    var4 = round(discount + 0.01, 2)
    var5 = 24

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)

    r = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)
                 & (lineitem['l_discount'] > var3) & (lineitem['l_discount'] < var4)
                 & (lineitem['l_quantity'] < var5)]

    r['value'] = r['l_extendedprice'] * (1 - r['l_discount'])

    r = r.agg(revenue=('value', 'sum'))

    print(r)

    pysdql.db_driver(db_path=sdql_database_path, name='tpch-6').run(r).export().to()
