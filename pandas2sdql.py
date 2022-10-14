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

    print(result)


def q3():
    """

    cu.sum(lambda p:
        {
            record({"c_custkey": p[0].c_custkey}):
            True
        }
        if
            p[0].c_mktsegment == "BUILDING"
        else
            None,
        True)
    ord.sum(lambda p:
        {
            record({"o_orderkey": p[0].o_orderkey,
                    "o_orderdate": p[0].o_orderdate,
                    "o_shippriority": p[0].o_shippriority}).concat(cu["o_custkey"]):
            True
        }
        if
            p[0].o_orderdate < "1995-03-15" and cu["o_custkey"] != None
        else
            None,
        True)
    li.sum(lambda p:
        {
            # There should be an aggregation.
        }
        True)

    :return:
    """


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

    print(result.operations)


if __name__ == '__main__':
    # q1()
    q6()
