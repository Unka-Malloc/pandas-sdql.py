from pysdql.core.dtypes.sdql_ir import *


class SDQLInspector:
    def __init__(self, ):
        pass

    @staticmethod
    def concat_cond(sdql_obj, cond, mode='and'):
        if mode == 'and':
            return IfExpr(condExpr=MulExpr(sdql_obj.condExpr, cond),
                          thenBodyExpr=sdql_obj.thenBodyExpr,
                          elseBodyExpr=sdql_obj.elseBodyExpr)

        if mode == 'or':
            return IfExpr(condExpr=AddExpr(sdql_obj.condExpr, cond),
                          thenBodyExpr=sdql_obj.thenBodyExpr,
                          elseBodyExpr=sdql_obj.elseBodyExpr)

    @staticmethod
    def add_cond(sdql_obj, cond, layer='outer'):
        if type(sdql_obj) == IfExpr:
            if layer == 'outer':
                return IfExpr(condExpr=cond,
                              thenBodyExpr=sdql_obj,
                              elseBodyExpr=sdql_obj.elseBodyExpr)

            if layer == 'inner':
                return IfExpr(condExpr=sdql_obj.condExpr,
                              thenBodyExpr=IfExpr(condExpr=cond,
                                                  thenBodyExpr=sdql_obj.thenBodyExpr,
                                                  elseBodyExpr=sdql_obj.elseBodyExpr),
                              elseBodyExpr=sdql_obj.elseBodyExpr)

    @staticmethod
    def findall_var(sdql_obj):
        all_var = []
        if type(sdql_obj) == ConstantExpr:
            all_var.append(sdql_obj)

        if type(sdql_obj) == LetExpr:
            let_var = sdql_obj.varExpr
            let_val = sdql_obj.valExpr
            let_body = sdql_obj.bodyExpr

            all_var += SDQLInspector.findall_var(let_var)
            all_var += SDQLInspector.findall_var(let_val)
            all_var += SDQLInspector.findall_var(let_body)

        if type(sdql_obj) == SumExpr:
            sum_el = sdql_obj.varExpr
            sum_on = sdql_obj.dictExpr
            sum_op = sdql_obj.bodyExpr

            all_var += SDQLInspector.findall_var(sum_el)
            all_var += SDQLInspector.findall_var(sum_on)
            all_var += SDQLInspector.findall_var(sum_op)

        if type(sdql_obj) == IfExpr:
            if_cond = sdql_obj.condExpr
            if_then = sdql_obj.thenBodyExpr
            if_else = sdql_obj.elseBodyExpr

            all_var += SDQLInspector.findall_var(if_cond)
            all_var += SDQLInspector.findall_var(if_then)
            all_var += SDQLInspector.findall_var(if_else)

        if type(sdql_obj) == MulExpr:
            mul_el_1 = sdql_obj.op1Expr
            mul_el_2 = sdql_obj.op2Expr

            all_var += SDQLInspector.findall_var(mul_el_1)
            all_var += SDQLInspector.findall_var(mul_el_2)

        if type(sdql_obj) == AddExpr:
            add_el_1 = sdql_obj.op1Expr
            add_el_2 = sdql_obj.op2Expr

            all_var += SDQLInspector.findall_var(add_el_1)
            all_var += SDQLInspector.findall_var(add_el_2)

        if type(sdql_obj) == DivExpr:
            div_el_1 = sdql_obj.op1Expr
            div_el_2 = sdql_obj.op2Expr

            all_var += SDQLInspector.findall_var(div_el_1)
            all_var += SDQLInspector.findall_var(div_el_2)

        if type(sdql_obj) == SubExpr:
            sub_el_1 = sdql_obj.op1Expr
            sub_el_2 = sdql_obj.op2Expr

            all_var += SDQLInspector.findall_var(sub_el_1)
            all_var += SDQLInspector.findall_var(sub_el_2)

        if type(sdql_obj) == CompareExpr:
            cmp_left = sdql_obj.leftExpr
            cmp_right = sdql_obj.rightExpr

            all_var += SDQLInspector.findall_var(cmp_left)
            all_var += SDQLInspector.findall_var(cmp_right)

        if type(sdql_obj) == DicConsExpr:
            dict_items = sdql_obj.initialPairs

            for i in dict_items:
                all_var += SDQLInspector.findall_var(i)

        if type(sdql_obj) == DicLookupExpr:
            the_dict = sdql_obj.dicExpr
            the_key = sdql_obj.keyExpr

            all_var += SDQLInspector.findall_var(the_dict)
            all_var += SDQLInspector.findall_var(the_key)

        if type(sdql_obj) == RecConsExpr:
            rec_items = sdql_obj.initialPairs

            for i in rec_items:
                all_var += SDQLInspector.findall_var(i)

        if type(sdql_obj) == RecAccessExpr:
            the_rec = sdql_obj.recExpr

            all_var += SDQLInspector.findall_var(the_rec)

        if type(sdql_obj) == PairAccessExpr:
            dict_el = sdql_obj.pairExpr

            all_var += SDQLInspector.findall_var(dict_el)

        if type(sdql_obj) == ConcatExpr:
            rec1 = sdql_obj.rec1
            rec2 = sdql_obj.rec2

            all_var += SDQLInspector.findall_var(rec1)
            all_var += SDQLInspector.findall_var(rec2)

        if type(sdql_obj) == ExtFuncExpr:
            arg1 = sdql_obj.inp1
            arg2 = sdql_obj.inp2
            arg3 = sdql_obj.inp3

            all_var += SDQLInspector.findall_var(arg1)
            all_var += SDQLInspector.findall_var(arg2)
            all_var += SDQLInspector.findall_var(arg3)

        return all_var

    @staticmethod
    def findall_const(sdql_obj):
        all_const = []
        if type(sdql_obj) == ConstantExpr:
            all_const.append(sdql_obj)

        if type(sdql_obj) == LetExpr:
            let_var = sdql_obj.varExpr
            let_val = sdql_obj.valExpr
            let_body = sdql_obj.bodyExpr

            all_const += SDQLInspector.findall_const(let_var)
            all_const += SDQLInspector.findall_const(let_val)
            all_const += SDQLInspector.findall_const(let_body)

        if type(sdql_obj) == SumExpr:
            sum_el = sdql_obj.varExpr
            sum_on = sdql_obj.dictExpr
            sum_op = sdql_obj.bodyExpr

            all_const += SDQLInspector.findall_const(sum_el)
            all_const += SDQLInspector.findall_const(sum_on)
            all_const += SDQLInspector.findall_const(sum_op)

        if type(sdql_obj) == IfExpr:
            if_cond = sdql_obj.condExpr
            if_then = sdql_obj.thenBodyExpr
            if_else = sdql_obj.elseBodyExpr

            all_const += SDQLInspector.findall_const(if_cond)
            all_const += SDQLInspector.findall_const(if_then)
            all_const += SDQLInspector.findall_const(if_else)

        if type(sdql_obj) == MulExpr:
            mul_el_1 = sdql_obj.op1Expr
            mul_el_2 = sdql_obj.op2Expr

            all_const += SDQLInspector.findall_const(mul_el_1)
            all_const += SDQLInspector.findall_const(mul_el_2)

        if type(sdql_obj) == AddExpr:
            add_el_1 = sdql_obj.op1Expr
            add_el_2 = sdql_obj.op2Expr

            all_const += SDQLInspector.findall_const(add_el_1)
            all_const += SDQLInspector.findall_const(add_el_2)

        if type(sdql_obj) == DivExpr:
            div_el_1 = sdql_obj.op1Expr
            div_el_2 = sdql_obj.op2Expr

            all_const += SDQLInspector.findall_const(div_el_1)
            all_const += SDQLInspector.findall_const(div_el_2)

        if type(sdql_obj) == SubExpr:
            sub_el_1 = sdql_obj.op1Expr
            sub_el_2 = sdql_obj.op2Expr

            all_const += SDQLInspector.findall_const(sub_el_1)
            all_const += SDQLInspector.findall_const(sub_el_2)

        if type(sdql_obj) == CompareExpr:
            cmp_left = sdql_obj.leftExpr
            cmp_right = sdql_obj.rightExpr

            all_const += SDQLInspector.findall_const(cmp_left)
            all_const += SDQLInspector.findall_const(cmp_right)

        if type(sdql_obj) == DicConsExpr:
            dict_items = sdql_obj.initialPairs

            for i in dict_items:
                all_const += SDQLInspector.findall_const(i)

        if type(sdql_obj) == DicLookupExpr:
            the_dict = sdql_obj.dicExpr
            the_key = sdql_obj.keyExpr

            all_const += SDQLInspector.findall_const(the_dict)
            all_const += SDQLInspector.findall_const(the_key)

        if type(sdql_obj) == RecConsExpr:
            rec_items = sdql_obj.initialPairs

            for i in rec_items:
                all_const += SDQLInspector.findall_const(i)

        if type(sdql_obj) == RecAccessExpr:
            the_rec = sdql_obj.recExpr

            all_const += SDQLInspector.findall_const(the_rec)

        if type(sdql_obj) == PairAccessExpr:
            dict_el = sdql_obj.pairExpr

            all_const += SDQLInspector.findall_const(dict_el)

        if type(sdql_obj) == ConcatExpr:
            rec1 = sdql_obj.rec1
            rec2 = sdql_obj.rec2

            all_const += SDQLInspector.findall_const(rec1)
            all_const += SDQLInspector.findall_const(rec2)

        if type(sdql_obj) == ExtFuncExpr:
            arg1 = sdql_obj.inp1
            arg2 = sdql_obj.inp2
            arg3 = sdql_obj.inp3

            all_const += SDQLInspector.findall_const(arg1)
            all_const += SDQLInspector.findall_const(arg2)
            all_const += SDQLInspector.findall_const(arg3)

        return all_const
