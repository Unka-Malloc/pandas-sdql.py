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
        left_opt = self.left.get_opt(OptGoal.MergeLeftPart)

        if left_opt.has_cond:
            part_left = IfExpr(condExpr=left_opt.get_cond_ir(),
                               thenBodyExpr=DicConsExpr([(
                                   RecConsExpr([(self.left_on, self.left.key_access(self.left_on))]),
                                   left_opt.get_col_proj_ir()
                               )]),
                               elseBodyExpr=ConstantExpr(None))
        else:
            part_left = DicConsExpr([(
                self.left.key_access(self.left_on),
                left_opt.get_col_proj_ir()
            )])

        print(left_opt.info)
        print(part_left)
        print(GenerateSDQLCode(part_left))

        right_opt = self.right.get_opt(OptGoal.MergeRightPart)

        right_col_out = right_opt.get_col_proj_ir()

        if right_opt.has_cond:
            part_right = IfExpr(condExpr=right_opt.get_cond_ir(),
                                thenBodyExpr=IfExpr(condExpr=DicLookupExpr(dicExpr=self.var_part_left,
                                                                           keyExpr=self.right.key_access(self.right_on))
                                                             != ConstantExpr(None),
                                                    thenBodyExpr=DicConsExpr([(
                                                        RecConsExpr([(self.right_on,
                                                                      self.right.key_access(self.right_on))]),
                                                        right_col_out
                                                    )]),
                                                    elseBodyExpr=ConstantExpr(None)),
                                elseBodyExpr=ConstantExpr(None))
        else:
            part_right = IfExpr(condExpr=DicLookupExpr(dicExpr=self.var_part_left,
                                                                           keyExpr=self.right.key_access(self.right_on))
                                                             != ConstantExpr(None),
                                thenBodyExpr=DicConsExpr([(
                                                        RecConsExpr([(self.right_on,
                                                                      self.right.key_access(self.right_on))]),
                                                        right_col_out
                                                    )]),
                                elseBodyExpr=EmptyDicConsExpr())

        print(right_opt.info)
        print(part_right)
        print(GenerateSDQLCode(part_right))

        # LetExpr(varExpr=self.var_part_left,
        #         valExpr=part_left,
        #         bodyExpr=)

        return left_opt.info

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
