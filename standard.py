def tpch_1(li):
    li_filt = li[(li.l_shipdate <= "1998-09-02")]

    li_filt["disc_price"] = li_filt.l_extendedprice * (1 - li_filt.l_discount)
    li_filt["charge"] = li_filt.l_extendedprice * (1 - li_filt.l_discount) * (1 + li_filt.l_tax)

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"]) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count"))
    return result

def func_tpch_1(li):
    lineitem_probed = li.sum(lambda p:
        {
            record({"l_returnflag": p[0].l_returnflag, "l_linestatus": p[0].l_linestatus}):
            record({"sum_qty": p[0].l_quantity, "sum_base_price": p[0].l_extendedprice, "sum_disc_price": (p[0].l_extendedprice * (1.0 - p[0].l_discount)), "sum_charge": ((p[0].l_extendedprice * (1.0 - p[0].l_discount)) * (1.0 + p[0].l_tax)), "count_order": 1})
        }
        if
            p[0].l_shipdate <= 19980902
        else
            None,
        True
        )

    results = lineitem_probed.sum(lambda p: {p[0].concat(p[1]): True}, False)