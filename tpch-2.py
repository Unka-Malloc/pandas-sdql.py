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
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
"""
import pysdql

if __name__ == '__main__':
    part = pysdql.Relation(name='part', cols=pysdql.PART_COLS)
    supplier = pysdql.Relation(name='supplier', cols=pysdql.SUPPLIER_COLS)
    partsupp = pysdql.Relation(name='partsupp', cols=pysdql.PARTSUPP_COLS)
    nation = pysdql.Relation(name='nation', cols=pysdql.NATION_COLS)
    region = pysdql.Relation(name='region', cols=pysdql.REGION_COLS)

