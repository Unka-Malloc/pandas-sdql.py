# Q1
```python
from pysdql.core.dtypes.sdql_ir import (
    LetExpr,
    SumExpr,
    IfExpr,
    MulExpr,
    CompareExpr,
    CompareSymbol,
    RecAccessExpr,
    PairAccessExpr,
    ConstantExpr,
    VarExpr,
    DicConsExpr,
    RecConsExpr,
    AddExpr,
    SumBuilder,
    ConcatExpr
)

LetExpr(lineitem_probed, 
        SumBuilder(lambda p: IfExpr((p[0].l_shipdate <= ConstantExpr(19980902)), 
                                    DicConsExpr([(RecConsExpr([("l_returnflag", p[0].l_returnflag), 
                                                               ("l_linestatus", p[0].l_linestatus)
                                                               ]), 
                                                  RecConsExpr([("sum_qty", p[0].l_quantity), 
                                                               ("sum_base_price", p[0].l_extendedprice), 
                                                               ("sum_disc_price", (p[0].l_extendedprice * (ConstantExpr(1.0) - p[0].l_discount))), 
                                                               ("sum_charge", ((p[0].l_extendedprice * (ConstantExpr(1.0) - p[0].l_discount)) * (ConstantExpr(1.0) + p[0].l_tax))), 
                                                               ("count_order", ConstantExpr(1))
                                                               ])
                                                  )]), 
                                    ConstantExpr(None)
                                    ), 
                   li, 
                   False
                   ), 
        LetExpr(results, 
                SumBuilder(lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]), 
                           lineitem_probed, 
                           True
                           ), 
                LetExpr(VarExpr("out"), results, ConstantExpr(True))))
```

# Q6
```python
from pysdql.core.dtypes.sdql_ir import (
    IfExpr,
    LetExpr,
    SumExpr,
    SumBuilder,
    MulExpr,
    CompareExpr,
    CompareSymbol,
    RecAccessExpr,
    PairAccessExpr,
    ConstantExpr,
    VarExpr
)

LetExpr(results, 
        SumBuilder(lambda p: IfExpr(
                                (
                                    ((p[0].l_shipdate >= ConstantExpr(19940101))) 
                                    * ((p[0].l_shipdate < ConstantExpr(19950101))) 
                                    * ((p[0].l_discount >= ConstantExpr(0.05))) 
                                    * ((p[0].l_discount <= ConstantExpr(0.07))) 
                                    * ((p[0].l_quantity < ConstantExpr(24.0)))
                                ), 
                             (p[0].l_extendedprice * p[0].l_discount), 
                             ConstantExpr(0.0)), 
                             li, ), 
        LetExpr(VarExpr("out"), results, ConstantExpr(True))
)
```