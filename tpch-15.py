"""
WITH revenue (supplier_no, total_revenue) as (
SELECT
l_suppkey,
SUM(l_extendedprice * (1-l_discount))
FROM
lineitem
WHERE
l_shipdate >= date ':1'
AND l_shipdate < date ':1' + interval '3' month
GROUP BY
l_suppkey
)
SELECT
s_suppkey,
s_name,
s_address,
s_phone,
total_revenue
FROM
supplier,
revenue
WHERE
s_suppkey = supplier_no
AND total_revenue = (
SELECT
MAX(total_revenue)
FROM
revenue
)
"""
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

    var1 = '1996-02-01'
    var2 = '1996-05-01'  # var1 + 3 month

    lineitem = pd.read_table(rf'{data_path}/lineitem.tbl', sep='|', index_col=False, header=None, names=pysdql.LINEITEM_COLS)
    supplier = pd.read_table(rf'{data_path}/supplier.tbl', sep='|', index_col=False, header=None, names=pysdql.SUPPLIER_COLS)

    sub_l = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)]
    sub_l.columns.name = 'sub_l'

    sub_l['tmp_val'] = sub_l['l_extendedprice'] * (1 - sub_l['l_discount'])

    revenue0 = sub_l.groupby(['l_suppkey'], as_index=False).agg(total_revenue=('tmp_val', 'sum'))
    revenue0['supplier_no'] = revenue0['l_suppkey']
    revenue0 = revenue0[['supplier_no', 'total_revenue']]
    revenue0.columns.name = 'revenue0'

    max_revenue = revenue0['total_revenue'].max()

    r = supplier.merge(revenue0, left_on='s_suppkey', right_on='supplier_no')

    r = r[r['total_revenue'] == max_revenue]

    r = r[['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']]

    print(r)

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-15').run(r).export().to()
