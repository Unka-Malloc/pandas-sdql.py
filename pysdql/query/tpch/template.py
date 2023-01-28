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
