from pysdql.core.dtypes.DataFrame import DataFrame as pysdqlDataFrame
import pysdql as pd


class tpch:
    @staticmethod
    def q1() -> pysdqlDataFrame:
        lineitem = pysdqlDataFrame()

        li_filt = lineitem[(lineitem['l_shipdate'] <= "1998-09-02")]
        li_filt["disc_price"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])
        li_filt["charge"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount']) * (1.0 + li_filt['l_tax'])

        result = li_filt \
            .groupby(["l_returnflag", "l_linestatus"]) \
            .agg(sum_qty=("l_quantity", "sum"),
                 sum_base_price=("l_extendedprice", "sum"),
                 sum_disc_price=("disc_price", "sum"),
                 sum_charge=("charge", "sum"),
                 count_order=("l_quantity", "count"))

        return result

    @staticmethod
    def q3() -> pysdqlDataFrame:
        orders = pysdqlDataFrame()
        customer = pysdqlDataFrame()
        lineitem = pysdqlDataFrame()

        cu_filt = customer[customer.c_mktsegment == "BUILDING"]

        ord_filt = orders[orders.o_orderdate < "1995-03-15"]
        ord_cu_join = pd.merge(cu_filt, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")

        li_filt = lineitem[lineitem.l_shipdate > "1995-03-15"]
        li_order_join = pd.merge(ord_cu_join, li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")
        li_order_join["revenue"] = li_order_join.l_extendedprice * (1.0 - li_order_join.l_discount)

        result = li_order_join \
            .groupby(["l_orderkey", "o_orderdate", "o_shippriority"]) \
            .agg({'revenue': 'sum'})

        return result

    @staticmethod
    def q4() -> pysdqlDataFrame:
        orders = pysdqlDataFrame()
        lineitem = pysdqlDataFrame()

        li_filt = lineitem[lineitem.l_commitdate < lineitem.l_receiptdate]
        li_proj = li_filt[["l_orderkey"]]

        ord_filt = orders[(orders.o_orderdate >= "1993-07-01")
                          & (orders.o_orderdate < "1993-10-01")
                          & orders.o_orderkey.isin(li_proj["l_orderkey"])]

        results = ord_filt \
            .groupby(["o_orderpriority"]) \
            .agg(order_count=("o_orderdate", "count"))

        return results

    @staticmethod
    def q6() -> pysdqlDataFrame:
        """

        :return: a singleton dictionary {record -> true}
        """
        lineitem = pysdqlDataFrame()

        li_filt = lineitem[
            (lineitem.l_shipdate >= "1994-01-01") &
            (lineitem.l_shipdate < "1995-01-01") &
            (lineitem.l_discount >= 0.05) &
            (lineitem.l_discount <= 0.07) &
            (lineitem.l_quantity < 24)
            ]

        li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

        result = li_filt.agg({'revenue': 'sum'})

        return result

    @staticmethod
    def q6_1() -> pysdqlDataFrame:
        """
        agg(revenue=('revenue', 'sum'))
        should be the same as
        agg({'revenue': 'sum'})
        :return: a singleton dictionary {record -> true}
        """
        lineitem = pysdqlDataFrame()

        li_filt = lineitem[
            (lineitem.l_shipdate >= "1994-01-01") &
            (lineitem.l_shipdate < "1995-01-01") &
            (lineitem.l_discount >= 0.05) &
            (lineitem.l_discount <= 0.07) &
            (lineitem.l_quantity < 24)
            ]

        li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

        result = li_filt.agg(total_revenue=('revenue', 'sum'))

        # result = li_filt.agg(order_count=('l_orderkey', 'count'))

        # result = li_filt.agg(total_revenue=('revenue', 'sum'),
        #                      order_count=('l_orderkey', 'count'))

        return result

    @staticmethod
    def q6_2() -> pysdqlDataFrame:
        """
        should return a single scalar
        :return:
        """
        lineitem = pysdqlDataFrame()

        li_filt = lineitem[
            (lineitem.l_shipdate >= "1994-01-01") &
            (lineitem.l_shipdate < "1995-01-01") &
            (lineitem.l_discount >= 0.05) &
            (lineitem.l_discount <= 0.07) &
            (lineitem.l_quantity < 24)
            ]

        li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

        result = li_filt['revenue'].sum()

        return result

    @staticmethod
    def q10() -> pysdqlDataFrame:
        orders = pysdqlDataFrame()
        customer = pysdqlDataFrame()
        nation = pysdqlDataFrame()
        lineitem = pysdqlDataFrame()

        ord_filt = orders[(orders['o_orderdate'] >= "1993-10-01") & (orders['o_orderdate'] < "1994-01-01")]

        ord_cu_join = pd.merge(customer, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")

        na_cu_join = pd.merge(nation, ord_cu_join, left_on="n_nationkey", right_on="c_nationkey", how="inner")
        na_cu_join = na_cu_join[
            ["o_orderkey", "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]]

        li_filt = lineitem[(lineitem['l_returnflag'] == "R")]

        li_ord_join = pd.merge(na_cu_join, li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")

        li_ord_join["revenue"] = li_ord_join.l_extendedprice * (1.0 - li_ord_join.l_discount)

        result = li_ord_join \
            .groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]) \
            .agg({"revenue": 'sum'})

        return result

    @staticmethod
    def q14() -> pysdqlDataFrame:
        lineitem = pysdqlDataFrame()
        part = pysdqlDataFrame()

        li_filt = lineitem[(lineitem['l_shipdate'] >= "1995-09-01") & (lineitem['l_shipdate'] < "1995-10-01")]

        li_pa_join = pd.merge(part, li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")

        li_pa_join["A"] = li_pa_join.apply(
            lambda x: x["l_extendedprice"] * (1.0 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0.0,
            axis=1)
        li_pa_join["B"] = li_pa_join['l_extendedprice'] * (1.0 - li_pa_join['l_discount'])

        result = li_pa_join['A'].sum() / li_pa_join['B'].sum() * 100.0

        return result

    @staticmethod
    def q15() -> pysdqlDataFrame:
        lineitem = pysdqlDataFrame()
        supplier = pysdqlDataFrame()

        li_filt = lineitem[(lineitem['l_shipdate'] >= "1996-01-01") & (lineitem['l_shipdate'] < "1996-04-01")]
        li_filt["revenue"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])

        li_aggr = li_filt \
            .groupby(["l_suppkey"]) \
            .agg(total_revenue=("revenue", "sum"))
        li_aggr = li_aggr[li_aggr['total_revenue'] == 1772627.2087]

        su_proj = supplier[["s_suppkey", "s_name", "s_address", "s_phone"]]
        li_su_join = pd.merge(su_proj, li_aggr, left_on="s_suppkey", right_on="l_suppkey", how="inner")

        result = li_su_join[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]]

        return result

    @staticmethod
    def q16() -> pysdqlDataFrame:
        part = pysdqlDataFrame()
        supplier = pysdqlDataFrame()
        partsupp = pysdqlDataFrame()

        pa_filt = part[
            (part.p_brand != "Brand#45") &
            (part.p_type.str.startswith("MEDIUM POLISHED") == False) &
            (
                    (part.p_size == 49) |
                    (part.p_size == 14) |
                    (part.p_size == 23) |
                    (part.p_size == 45) |
                    (part.p_size == 19) |
                    (part.p_size == 3) |
                    (part.p_size == 36) |
                    (part.p_size == 9)
            )]
        pa_proj = pa_filt[["p_partkey", "p_brand", "p_type", "p_size"]]

        su_filt = supplier[
            supplier.s_comment.str.contains("Customer")
            & ((supplier.s_comment.str.find("Customer") + 7) < supplier.s_comment.str.find("Complaints"))]
        su_proj = su_filt[["s_suppkey"]]

        ps_filt = partsupp[~partsupp.ps_suppkey.isin(su_proj["s_suppkey"])]

        ps_pa_join = pd.merge(pa_proj, ps_filt, left_on="p_partkey", right_on="ps_partkey", how="inner")

        result = ps_pa_join \
            .groupby(["p_brand", "p_type", "p_size"]) \
            .agg(supplier_cnt=("ps_suppkey", lambda x: x.nunique()))

        return result

    @staticmethod
    def q18() -> pysdqlDataFrame:
        orders = pysdqlDataFrame()
        customer = pysdqlDataFrame()
        lineitem = pysdqlDataFrame()

        li_aggr = lineitem \
            .groupby(["l_orderkey"]) \
            .agg(sum_quantity=("l_quantity", "sum"))

        li_filt = li_aggr[li_aggr.sum_quantity > 300].reset_index()
        li_proj = li_filt[["l_orderkey"]]

        ord_filt = orders[orders.o_orderkey.isin(li_proj["l_orderkey"])]

        cu_proj = customer[["c_custkey", "c_name"]]
        ord_cu_join = pd.merge(cu_proj, ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")
        ord_cu_join = ord_cu_join[["o_orderkey", "c_name", "c_custkey", "o_orderdate", "o_totalprice"]]

        li_ord_join = pd.merge(ord_cu_join, lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")

        result = li_ord_join \
            .groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]) \
            .agg(sum_quantity=("l_quantity", "sum"))

        return result

    @staticmethod
    def q19() -> pysdqlDataFrame:
        lineitem = pysdqlDataFrame()
        part = pysdqlDataFrame()

        pa_filt = part[
            ((part.p_brand == "Brand#12")
             & (part.p_container.isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"]))
             & (part.p_size >= 1) & (part.p_size <= 5)) |
            ((part.p_brand == "Brand#23")
             & (part.p_container.isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"]))
             & (part.p_size >= 1) & (part.p_size <= 10)) |
            ((part.p_brand == "Brand#34")
             & (part.p_container.isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"]))
             & (part.p_size >= 1) & (part.p_size <= 15))
            ]

        pa_proj = pa_filt[["p_partkey", "p_brand"]]

        li_filt = lineitem[(((lineitem.l_shipmode == "AIR") | (lineitem.l_shipmode == "AIR REG"))
                            & (lineitem.l_shipinstruct == "DELIVER IN PERSON"))]

        li_pa_join = pd.merge(pa_proj, li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")
        li_pa_join_filt = li_pa_join[
            (
                    ((li_pa_join.p_brand == "Brand#12")
                       & ((li_pa_join.l_quantity >= 1) & (li_pa_join.l_quantity <= 11)))
                    | ((li_pa_join.p_brand == "Brand#23")
                       & ((li_pa_join.l_quantity >= 10) & (li_pa_join.l_quantity <= 20)))
                    | ((li_pa_join.p_brand == "Brand#34")
                       & ((li_pa_join.l_quantity >= 20) & (li_pa_join.l_quantity <= 30)))
            )
        ]

        li_pa_join_filt["revenue"] = li_pa_join_filt['l_extendedprice'] * (1.0 - li_pa_join_filt['l_discount'])

        result = li_pa_join_filt.agg({'revenue': 'sum'})

        return result
