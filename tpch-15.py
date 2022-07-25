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

if __name__ == '__main__':
    var1 = '1996-02-01'
    var2 = '1996-05-01'  # var1 + 3 month

    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', header=pysdql.LINEITEM_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)

    sub_l = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)].rename('sub_l')

    revenue0 = sub_l.groupby(['l_suppkey']) \
        .agg(supplier_no=sub_l['l_suppkey'],
             total_revenue=(sub_l['l_extendedprice'] * (1 - sub_l['l_discount']), 'sum')) \
        .rename('revenue0')

    max_revenue = revenue0.agg({revenue0['total_revenue']: 'max'}).rename('max_revenue')

    r = supplier.merge(revenue0, on=(supplier['s_suppkey'] == revenue0['supplier_no'])
                                    & (revenue0['total_revenue'] == max_revenue))

    r = r[['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']]

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-15').run(r).export().to()
