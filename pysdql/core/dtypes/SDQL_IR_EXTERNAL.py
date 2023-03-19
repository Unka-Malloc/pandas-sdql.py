from sdql_ir import (
    ConstantExpr,
    VarExpr,
    SumExpr,
    DicConsExpr,
    RecAccessExpr,
    PairAccessExpr,
)

class UniqueBuild:
    def __init__(self, key, field):
        self.key = key
        self.field = field

    @property
    def sdql_ir(self):
        return DicConsExpr([(RecAccessExpr(recExpr=self.key, fieldName=self.field), ConstantExpr(True))])

class UniqueProbe:
    def __init__(self, key, field):
        self.key = key
        self.field = field

    @property
    def sdql_ir(self):
        tmp_var = VarExpr(f'y_{self.field}')
        return SumExpr(varExpr=tmp_var,
                       dictExpr=RecAccessExpr(recExpr=self.key, fieldName=self.field),
                       bodyExpr=PairAccessExpr(tmp_var, 0),
                       isAssignmentSum=True)