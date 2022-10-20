from pysdql.core.dtypes.EnumUtil import OptGoal
from pysdql.core.dtypes.sdql_ir import LetExpr, VarExpr, IfExpr, DicConsExpr, RecConsExpr, ConstantExpr, \
    EmptyDicConsExpr, DicLookupExpr, PrintAST, GenerateSDQLCode


class MergeExpr:
    def __init__(self, left, right, how, left_on, right_on):
        self.left = left
        self.right = right
        self.how = how
        self.left_on = left_on
        self.right_on = right_on

        self.var_part_left = VarExpr(f'part_{left.name}')
        self.var_merged = VarExpr(f'{left.name}_merge_{right.name}')

    @property
    def sdql_ir(self):
        return self.right_on.merge_probe_stmt

    def __repr__(self):
        return str({
            'left': self.left,
            'right': self.right,
            'how': self.how,
            'left_on': self.left_on,
            'right_on': self.right_on
        })

    @property
    def op_name_suffix(self):
        return f'_merge'
