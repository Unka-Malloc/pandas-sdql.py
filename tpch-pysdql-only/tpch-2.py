"""
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = :1
	and p_type like '%:2'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = ':3'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = ':3'
	)
"""
import pysdql

if __name__ == '__main__':
    var1 = 14
    var2 = 'BRASS'
    var3 = 'EUROPE'

    part = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/part.tbl', names=pysdql.PART_COLS)
    supplier = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/supplier.tbl', names=pysdql.SUPPLIER_COLS)
    lineitem = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/lineitem.tbl', names=pysdql.LINEITEM_COLS)
    partsupp = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/partsupp.tbl', names=pysdql.PARTSUPP_COLS)
    nation = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/nation.tbl', names=pysdql.NATION_COLS)
    region = pysdql.read_tbl(path=r'T:/UG4-Proj/datasets/region.tbl', names=pysdql.REGION_COLS)

    sub_r = region[region['r_name'] == var3].rename('sub_r')

    r1 = nation.merge(sub_r, on=nation['n_regionkey'] == sub_r['r_regionkey'])
    r1 = r1.merge(supplier, on=r1['n_nationkey'] == supplier['s_nationkey'])
    r1 = r1.merge(partsupp, on=r1['s_suppkey'] == partsupp['ps_suppkey'])
    r1 = r1.groupby(['ps_partkey', 'ps_suppkey']).agg(min_partkey=r1['ps_partkey'],
                                                      min_suppkey=r1['ps_suppkey'],
                                                      min_supplycost=(r1['ps_supplycost'], 'min')).rename('r1')

    sub_p = part[part['p_size'] == var1]
    sub_p = sub_p[sub_p['p_type'].endswith(var2)].rename('sub_p')

    r2 = nation.merge(sub_r, on=nation['n_regionkey'] == sub_r['r_regionkey'])
    r2 = r2.merge(supplier, on=r2['n_nationkey'] == supplier['s_nationkey'])
    r2 = r2.merge(partsupp, on=r2['s_suppkey'] == partsupp['ps_suppkey'])
    r2 = r2.merge(sub_p, on=r2['ps_partkey'] == sub_p['p_partkey'], name='r2')

    r = r1.merge(r2, on=(r1['min_partkey'] == r2['ps_partkey'])
                        & (r1['min_suppkey'] == r2['ps_suppkey'])
                        & (r1['min_supplycost'] == r2['ps_supplycost']))

    r = r[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]

    pysdql.db_driver(db_path=r'T:/sdql', name='tpch-2').run(r)
