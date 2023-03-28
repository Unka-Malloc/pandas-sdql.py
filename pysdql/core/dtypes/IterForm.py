from pysdql.core.dtypes import NewColOpExpr, AggrExpr, OldColOpExpr, ColExtExpr
from pysdql.core.dtypes.CalcExpr import CalcExpr
from pysdql.core.dtypes.ColApplyExpr import ColApplyExpr
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.dtypes.sdql_ir import VarExpr, SumExpr, IfExpr, ConstantExpr, DicConsExpr, PairAccessExpr, ConcatExpr, \
    RecAccessExpr, RecConsExpr, Expr, CompareExpr, CompareSymbol, MulExpr, AddExpr, DivExpr, SubExpr
from pysdql.extlib.sdqlir_to_sdqlpy import GenerateSDQLPYCode
from pysdql.extlib.sdqlpy.sdql_lib import sr_dict


class IterForm:
    def __init__(self, iter_on: str, iter_el: str):
        if not isinstance(iter_on, str):
            raise TypeError(f'iter_on must be str type.')
        if not isinstance(iter_el, str):
            raise TypeError(f'iter_on must be str type.')

        self.iter_on = iter_on
        self.iter_el = iter_el
        self.iter_on_obj = VarExpr(self.iter_on)
        self.iter_el_obj = VarExpr(self.iter_el)
        self.iter_key = PairAccessExpr(VarExpr(self.iter_el), 0)
        self.iter_val = PairAccessExpr(VarExpr(self.iter_el), 1)
        self.iter_cond = []
        self.iter_op = None
        self.iter_else = None

    @property
    def iter_body(self):
        if self.iter_op:
            if isinstance(self.iter_op, Expr):
                return self.iter_op
            if isinstance(self.iter_op, sr_dict):
                return self.iter_op
            if isinstance(self.iter_op, NewColOpExpr):
                col = self.iter_op
                if isinstance(col.col_expr, ColExtExpr):
                    return DicConsExpr([(ConcatExpr(self.iter_key,
                                                    RecConsExpr([(col.col_var,
                                                                  col.col_expr.replace(self.iter_key).sdql_ir)])),
                                         PairAccessExpr(VarExpr(self.iter_el), 1))])
                elif isinstance(col.col_expr, ColApplyExpr):
                    if col.col_expr.original_column:
                        return DicConsExpr([(ConcatExpr(self.iter_key,
                                                        RecConsExpr([(col.col_var,
                                                                      SDQLInspector.replace_access(
                                                                          col.col_expr.original_unopt_sdql_ir,
                                                                          PairAccessExpr(VarExpr(self.iter_el), 0))
                                                                      )])
                                                        ),
                                             PairAccessExpr(VarExpr(self.iter_el), 1))])

                    return DicConsExpr([(ConcatExpr(self.iter_key,
                                                    RecConsExpr([(col.col_var,
                                                                  SDQLInspector.replace_access(col.col_expr.unopt_sdql_ir,
                                                                                               PairAccessExpr(VarExpr(self.iter_el),
                                                                                                              0)))])),
                                         PairAccessExpr(VarExpr(self.iter_el), 1))])
                elif isinstance(col.col_expr, (IfExpr, MulExpr, AddExpr, DivExpr, SubExpr)):
                    return DicConsExpr([(ConcatExpr(self.iter_key,
                                                    RecConsExpr([(col.col_var,
                                                                  SDQLInspector.replace_access(col.col_expr,
                                                                                              PairAccessExpr(VarExpr(self.iter_el),
                                                                                                              0)))])),
                                         PairAccessExpr(VarExpr(self.iter_el), 1))])
                else:
                    return DicConsExpr([(ConcatExpr(self.iter_key,
                                                    RecConsExpr([(col.col_var,
                                                                  col.col_expr.replace(self.iter_key))])),
                                         PairAccessExpr(VarExpr(self.iter_el), 1))])
            if isinstance(self.iter_op, OldColOpExpr):
                col = self.iter_op
                if isinstance(col.col_expr, ColApplyExpr):
                    return DicConsExpr([(ConcatExpr(self.iter_key,
                                                    RecConsExpr([(col.col_var,
                                                                  SDQLInspector.replace_access(col.col_expr.unopt_sdql_ir,
                                                                                               PairAccessExpr(VarExpr(self.iter_el),
                                                                                                              0)))])),
                                         PairAccessExpr(VarExpr(self.iter_el), 1))])
                else:
                    return DicConsExpr([(ConcatExpr(self.iter_key,
                                                    RecConsExpr([(col.col_expr,
                                                                  RecAccessExpr(self.iter_key,
                                                                                col.col_var))])),
                                         PairAccessExpr(VarExpr(self.iter_el), 1))])

            print(f'Unexpected operation in type {type(self.iter_op)}')

        return DicConsExpr([(PairAccessExpr(VarExpr(self.iter_el), 0), PairAccessExpr(VarExpr(self.iter_el), 1))])

    @property
    def sdql_ir(self):
        res_op = self.iter_body

        if self.iter_cond:
            for cond in self.iter_cond:
                if isinstance(cond, FlexIR):
                    if cond.replaceable:
                        if isinstance(cond, ColExtExpr):
                            # print(cond)
                            cond = cond.replace(self.iter_key).sdql_ir
                        else:
                            cond = cond.replace(self.iter_key)
                    else:
                        cond = cond.sdql_ir

                cond_else = self.iter_else if self.iter_else else ConstantExpr(None)

                res_op = IfExpr(condExpr=cond,
                                thenBodyExpr=res_op,
                                elseBodyExpr=cond_else)
        # else:
        #     res_op = IfExpr(condExpr=ConstantExpr(True),
        #                     thenBodyExpr=res_op,
        #                     elseBodyExpr=ConstantExpr(None))

        # extra_cond = CompareExpr(CompareSymbol.NE, self.iter_key, ConstantExpr(None))
        #
        # res_op = IfExpr(condExpr=extra_cond,
        #                 thenBodyExpr=res_op,
        #                 elseBodyExpr=ConstantExpr(None))

        return SumExpr(varExpr=self.iter_el_obj,
                       dictExpr=self.iter_on_obj,
                       bodyExpr=res_op,
                       isAssignmentSum=False)

    def __repr__(self):
        return GenerateSDQLPYCode(self.sdql_ir, {})
