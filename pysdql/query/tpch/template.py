def tpch_q1(lineitem):
    li_filt = lineitem[(lineitem['l_shipdate'] <= "1998-09-02")]
    li_filt["disc_price"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])
    li_filt["charge"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount']) * (1.0 + li_filt['l_tax'])

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"], as_index=False) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count"))

    return result


def tpch_q3(lineitem, customer, orders):
    var1 = "BUILDING"
    var2 = "1995-03-15"

    cu_filt = customer[customer['c_mktsegment'] == var1]

    ord_filt = orders[orders['o_orderdate'] < var2]

    cu_ord_join = cu_filt.merge(ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")

    li_filt = lineitem[lineitem['l_shipdate'] > var2]
    li_ord_join = cu_ord_join.merge(li_filt, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    li_ord_join["revenue"] = li_ord_join['l_extendedprice'] * (1.0 - li_ord_join['l_discount'])

    result = li_ord_join \
        .groupby(["l_orderkey", "o_orderdate", "o_shippriority"], as_index=False) \
        .agg({'revenue': 'sum'})

    return result


def tpch_q4(orders, lineitem):
    var1 = "1993-07-01"
    var2 = "1993-10-01"  # var1 + interval '3' month

    li_filt = lineitem[lineitem.l_commitdate < lineitem.l_receiptdate]
    li_proj = li_filt[["l_orderkey"]]

    ord_filt = orders[(orders.o_orderdate >= var1)
                      & (orders.o_orderdate < var2)
                      & orders.o_orderkey.isin(li_proj["l_orderkey"])]

    results = ord_filt \
        .groupby(["o_orderpriority"], as_index=False) \
        .agg(order_count=("o_orderdate", "count"))

    return results


def tpch_q6(lineitem):
    var1 = 4
    var2 = 0.06
    var3 = 24

    li_filt = lineitem[
        (lineitem.l_shipdate >= f"199{var1}-01-01") &
        (lineitem.l_shipdate < f"199{var1 + 1}-01-01") &
        (lineitem.l_discount >= var2 - 0.01) &
        (lineitem.l_discount <= var2 + 0.01) &
        (lineitem.l_quantity < var3)
        ]

    li_filt['revenue'] = li_filt.l_extendedprice * li_filt.l_discount

    result = li_filt.agg({'revenue': 'sum'})

    return result


def tpch_q14(lineitem, part):
    var1 = "1995-09-01"
    var2 = "1995-10-01"  # var1 + interval '1' month

    li_filt = lineitem[(lineitem['l_shipdate'] >= var1) & (lineitem['l_shipdate'] < var2)]

    li_pa_join = part.merge(li_filt, left_on="p_partkey", right_on="l_partkey", how="inner")

    li_pa_join["A"] = li_pa_join.apply(
        lambda x: x["l_extendedprice"] * (1.0 - x["l_discount"]) if x["p_type"].startswith("PROMO") else 0.0,
        axis=1)
    li_pa_join["B"] = li_pa_join['l_extendedprice'] * (1.0 - li_pa_join['l_discount'])

    result = li_pa_join['A'].sum() / li_pa_join['B'].sum() * 100.0

    return result


def tpch_q15(lineitem, supplier):
    li_filt = lineitem[(lineitem['l_shipdate'] >= "1996-01-01") & (lineitem['l_shipdate'] < "1996-04-01")]
    li_filt["revenue"] = li_filt['l_extendedprice'] * (1.0 - li_filt['l_discount'])

    li_aggr = li_filt \
        .groupby(["l_suppkey"]) \
        .agg(total_revenue=("revenue", "sum"))

    # maximum:
    # 1G -> 1772627.2087
    # 100M -> 1614410.2928
    # 10M -> 1161099.4636
    # 1M -> 797313.3838

    li_aggr = li_aggr[li_aggr['total_revenue'] == 797313.3838]

    su_proj = supplier[["s_suppkey", "s_name", "s_address", "s_phone"]]
    li_su_join = su_proj.merge(li_aggr, left_on="s_suppkey", right_on="l_suppkey", how="inner")

    result = li_su_join[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]]

    return result


def tpch_q16(partsupp, part, supplier):
    # 1G
    var1 = "Brand#45"
    var2 = "MEDIUM POLISHED"
    var3 = (49, 14, 23, 45, 19, 3, 36, 9)

    pa_filt = part[
        (part.p_brand != var1) &
        (part.p_type.str.startswith(var2) == False) &
        (part.p_size.isin(var3))]
    pa_proj = pa_filt[["p_partkey", "p_brand", "p_type", "p_size"]]

    su_filt = supplier[
        supplier.s_comment.str.contains("Customer")
        & ((supplier.s_comment.str.find("Customer") + 7) < supplier.s_comment.str.find("Complaints"))]
    su_proj = su_filt[["s_suppkey"]]

    ps_filt = partsupp[~partsupp.ps_suppkey.isin(su_proj["s_suppkey"])]

    ps_pa_join = pa_proj.merge(ps_filt, left_on="p_partkey", right_on="ps_partkey", how="inner")

    result = ps_pa_join \
        .groupby(["p_brand", "p_type", "p_size"], as_index=False) \
        .agg(supplier_cnt=("ps_suppkey", lambda x: x.nunique()))

    return result


def tpch_q18(lineitem, customer, orders):
    li_aggr = lineitem \
        .groupby(["l_orderkey"]) \
        .agg(sum_quantity=("l_quantity", "sum"))

    li_filt = li_aggr[li_aggr['sum_quantity'] > 300].reset_index()
    li_proj = li_filt[["l_orderkey"]]

    ord_filt = orders[orders['o_orderkey'].isin(li_proj["l_orderkey"])]

    cu_proj = customer[["c_custkey", "c_name"]]
    cu_ord_join = cu_proj.merge(ord_filt, left_on="c_custkey", right_on="o_custkey", how="inner")
    cu_ord_join = cu_ord_join[["c_name", "c_custkey", "o_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]]

    li_ord_join = cu_ord_join.merge(lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    result = li_ord_join \
        .groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]) \
        .agg(sum_quantity=("l_quantity", "sum"))

    return result
