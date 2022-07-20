"""
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> ':1'
	and p_type not like ':2%'
	and p_size in (:3, :4, :5, :6, :7, :8, :9, :10)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
"""
import pysdql

if __name__ == '__main__':
    var1 = 'Brand#21'
    var2 = 'SMALL ANODIZED'
    var3 = (48, 33, 18, 16, 8, 3, 10, 42)

    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', header=pysdql.SUPPLIER_COLS)
    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', header=pysdql.PART_COLS)
    partsupp = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/partsupp.tbl', header=pysdql.PARTSUPP_COLS)

    sub_s = supplier[supplier['s_comment'].contains('Customer', 'Complaints')].rename('sub_s')

    sub_p = part[(part['p_brand'] != 'Brand#21')
                 & (~part['p_type'].str.startswith('SMALL ANODIZED'))
                 & (part['p_size'].isin(var3))].rename('sub_p')

    r = sub_p.merge(partsupp, on=sub_p['p_partkey'] == partsupp['ps_partkey'])

    r = r[~(r['ps_suppkey'].isin(sub_s['s_suppkey']))]

    # COUNT (DISTINCT value) ?
    r = r.groupby(['p_brand', 'p_type', 'p_size']).aggr(supplier_cnt=(r['ps_suppkey'], 'sum'))

    pysdql.db_driver(db_path=r'T:/sdql').run(r, block=False)
