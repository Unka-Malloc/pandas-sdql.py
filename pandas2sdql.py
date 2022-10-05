from pysdql import DataFrame


def q1():
    li = DataFrame()

    li_filt = li[(li.l_shipdate <= "1998-09-02")]
    li_filt["disc_price"] = li_filt.l_extendedprice * (1 - li_filt.l_discount)
    li_filt["charge"] = li_filt.l_extendedprice * (1 - li_filt.l_discount) * (1 + li_filt.l_tax)

    result = li_filt \
        .groupby(["l_returnflag", "l_linestatus"]) \
        .agg(sum_qty=("l_quantity", "sum"),
             sum_base_price=("l_extendedprice", "sum"),
             sum_disc_price=("disc_price", "sum"),
             sum_charge=("charge", "sum"),
             count_order=("l_quantity", "count")
             )

    result.optimize()




def q6():
    # replaced by read_csv() in the future,
    # the name of the Dataframe will be set to the name of the csv file by default
    li = DataFrame()

    li_filt = li[
        (li.l_shipdate >= "1994-01-01") &
        (li.l_shipdate < "1995-01-01") &
        (li.l_discount >= 0.05) &
        (li.l_discount <= 0.07) &
        (li.l_quantity < 24)
        ]

    result = (li_filt.l_extendedprice * li_filt.l_discount).sum()

    result.optimize()


if __name__ == '__main__':
    q1()
    # q6()
