from pysdql.query.tpch.template import tpch_vars

duck_q1 = f"""
-- TPC-H Query 1
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
        l_shipdate <= date '{tpch_vars[1][0]}'
group by
        l_returnflag,
        l_linestatus
"""

duck_q2 = f"""
-- TPC-H Query 2
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
        and p_size = {tpch_vars[2][0]}
        and p_type like '%{tpch_vars[2][1]}'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '{tpch_vars[2][2]}'
        and ps_supplycost = (
                select
                        sum(ps_supplycost)
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
                        and r_name = '{tpch_vars[2][2]}'
        )
"""

duck_q3 = f"""
-- TPC-H Query 3
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = '{tpch_vars[3][0]}'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < DATE '{tpch_vars[3][1]}'
        and l_shipdate > DATE '{tpch_vars[3][1]}'
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
"""

duck_q4 = f"""
-- TPC-H Query 4
select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '{tpch_vars[4][0]}'
        and o_orderdate < date '{tpch_vars[4][1] }'
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
"""

duck_q5 = f"""
-- TPC-H Query 5
select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '{tpch_vars[5][0]}'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1995-01-01'
group by
        n_name
"""

duck_q6 = f"""
-- TPC-H Query 6
select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= date '{tpch_vars[6][0]}'
        and l_shipdate < date '{tpch_vars[6][1]}'
        and l_discount between {tpch_vars[6][2]} and {tpch_vars[6][3]}
        and l_quantity < {tpch_vars[6][4]}
"""

duck_q7 = f"""
-- TPC-H Query 7
select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                                (n1.n_name = '{tpch_vars[7][0]}' and n2.n_name = '{tpch_vars[7][1]}')
                                or (n1.n_name = '{tpch_vars[7][1]}' and n2.n_name = '{tpch_vars[7][0]}')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
"""

duck_q8 = f"""
-- TPC-H Query 8
select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = '{tpch_vars[8][1]}'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = '{tpch_vars[8][2]}'
        ) as all_nations
group by
        o_year
"""

duck_q9 = f"""
-- TPC-H Query 9
select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%{tpch_vars[9][0]}%'
        ) as profit
group by
        nation,
        o_year
"""

duck_q10 = f"""
-- TPC-H Query 10
select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '{tpch_vars[10][0]}'
        and o_orderdate < date '{tpch_vars[10][1]}'
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
"""

duck_q11 = f"""
-- TPC-H Query 11
select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as "value"
from
        partsupp,
        supplier,
        nation
where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                        select
                                sum(ps_supplycost * ps_availqty) * 0.0001
                        from
                                partsupp,
                                supplier,
                                nation
                        where
                                ps_suppkey = s_suppkey
                                and s_nationkey = n_nationkey
                                and n_name = '{tpch_vars[11][0]}'
                )
"""

duck_q12 = f"""
-- TPC-H Query 12
select
        l_shipmode,
        sum(case
                when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        orders,
        lineitem
where
        o_orderkey = l_orderkey
        and l_shipmode in ('{tpch_vars[12][0]}', '{tpch_vars[12][1]}')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '{tpch_vars[12][2]}'
        and l_receiptdate < date '{tpch_vars[12][3]}'
group by
        l_shipmode
"""

duck_q13 = f"""
-- TPC-H Query 13
select
        c_count,
        count(*) as custdist
from
        (
                select
                        c_custkey,
                        count(o_orderkey) c_count
                from
                        customer left outer join orders on
                                c_custkey = o_custkey
                                and o_comment not like '%{tpch_vars[13][0]}%{tpch_vars[13][1]}%'
                group by
                        c_custkey
        ) as c_orders
group by
        c_count
"""

duck_q14 = f"""
-- TPC-H Query 14
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
        and l_shipdate >= date '{tpch_vars[14][0]}'
        and l_shipdate < date '{tpch_vars[14][1]}'
"""

# q15 = """

# -- TPC-H Query 15

# with revenue as (
# 	select
# 		l_suppkey as supplier_no,
# 		sum(l_extendedprice * (1 - l_discount)) as total_revenue
# 	from
# 		lineitem
# 	where
# 		l_shipdate >= date '1996-01-01'
# 		and l_shipdate < date '1996-04-01'
# 	group by
# 		l_suppkey)
# select
# 	s_suppkey,
# 	s_name,
# 	s_address,
# 	s_phone,
# 	total_revenue
# from
# 	supplier,
# 	revenue
# where
# 	s_suppkey = supplier_no
# 	and total_revenue = (
# 		select
# 			max(total_revenue)
# 		from
# 			revenue
# 	)

# """


duck_q15 = f"""
-- TPC-H Query 15
with revenue as (
	select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		lineitem
	where
		l_shipdate >= date '{tpch_vars[15][0]}'
		and l_shipdate < date '{tpch_vars[15][1]}'
	group by
		l_suppkey)
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue
where
	s_suppkey = supplier_no
	and total_revenue = {tpch_vars[15][2]}
"""

duck_q16 = f"""
-- TPC-H Query 16
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
        and p_brand <> '{tpch_vars[16][0]}'
        and p_type not like '{tpch_vars[16][1]}%'
        and p_size in {tpch_vars[16][2]}
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

duck_q17 = """
-- TPC-H Query 17
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
        lineitem,
        part
where
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container = 'MED BOX'
        and l_quantity < (
                select
                        0.2 * avg(l_quantity)
                from
                        lineitem
                where
                        l_partkey = p_partkey
        )
"""

duck_q18 = """
-- TPC-H Query 18
select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
from
        customer,
        orders,
        lineitem
where
        o_orderkey in (
                select
                        l_orderkey
                from
                        lineitem
                group by
                        l_orderkey having
                                sum(l_quantity) > 300
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
"""

duck_q19 = """
-- TPC-H Query 19
select
        sum(l_extendedprice* (1 - l_discount)) as revenue
from
        lineitem,
        part
where
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#12'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 1 and l_quantity <= 1 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#23'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 10 and l_quantity <= 10 + 10
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#34'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 20 and l_quantity <= 20 + 10
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
"""

duck_q20 = """
-- TPC-H Query 20
select
        s_name,
        s_address
from
        supplier,
        nation
where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        partsupp
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        part
                                where
                                        p_name like 'forest%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1994-01-01'
                                        and l_shipdate < date '1995-01-01'
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
"""

duck_q21 = """
-- TPC-H Query 21
select
        s_name,
        count(*) as numwait
from
        supplier,
        lineitem l1,
        orders,
        nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
group by
        s_name
"""

duck_q22 = """
-- TPC-H Query 22
select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_acctbal
                from
                        customer
                where
                        substring(c_phone from 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c_acctbal)
                                from
                                        customer
                                where
                                        c_acctbal > 0.00
                                        and substring(c_phone from 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        orders
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
group by
        cntrycode
"""
