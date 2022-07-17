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
    part = pysdql.relation(name='part', cols=pysdql.PART_COLS)
    supplier = pysdql.relation(name='supplier', cols=pysdql.SUPPLIER_COLS)
    partsupp = pysdql.relation(name='partsupp', cols=pysdql.PARTSUPP_COLS)
    nation = pysdql.relation(name='nation', cols=pysdql.NATION_COLS)
    region = pysdql.relation(name='region', cols=pysdql.REGION_COLS)

    aggr_val = pysdql.merge(part, partsupp, supplier, nation, region,
                            on=(part['p_partkey'] == partsupp['ps_partkey'])
                                & (partsupp['ps_suppkey'] == supplier['s_suppkey'])
                                & (supplier['s_nationkey'] == nation['n_nationkey'])
                                & (nation['n_regionkey'] == region['r_regionkey'])
                            )[region['r_name'] == ':3'].aggr({'ps_supplycost', 'min'})

    r = pysdql.merge(part, supplier, partsupp, nation, region,
                     on=(part['p_partkey'] == partsupp['ps_partkey'])
                        & (supplier['s_suppkey'] == partsupp['ps_suppkey'])
                        & (supplier['s_nationkey'] == nation['n_nationkey'])
                        & (nation['n_regionkey'] == region['r_regionkey'])
                     )[(part['p_size'] == 1)
                       & (region['r_name'] == ':3')
                       & (partsupp['ps_supplycost'] == aggr_val)][['s_acctbal',
                                                                   's_name',
                                                                   'n_name',
                                                                   'p_partkey',
                                                                   'p_mfgr',
                                                                   's_address',
                                                                   's_phone',
                                                                   's_comment']]
