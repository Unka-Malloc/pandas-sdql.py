from pysdql.core.dtypes.sdql_ir import *


def replace_var(sdql_obj, old_dict, new_dict):
    if type(sdql_obj) in (bool, int, float, str):
        return sdql_obj

    if type(sdql_obj) == ConstantExpr:
        return sdql_obj

    if type(sdql_obj) == VarExpr:
        if SDQLInspector.var_eq(sdql_obj, old_dict):
            return new_dict
        else:
            return sdql_obj

    if type(sdql_obj) == LetExpr:
        let_var = sdql_obj.varExpr
        let_val = sdql_obj.valExpr
        let_body = sdql_obj.bodyExpr

        return LetExpr(varExpr=SDQLInspector.replace_var(let_var, old_dict, new_dict),
                       valExpr=SDQLInspector.replace_var(let_val, old_dict, new_dict),
                       bodyExpr=SDQLInspector.replace_var(let_body, old_dict, new_dict))

    if type(sdql_obj) == SumExpr:
        sum_el = sdql_obj.varExpr
        sum_on = sdql_obj.dictExpr
        sum_op = sdql_obj.bodyExpr
        sum_assign = sdql_obj.isAssignmentSum

        return SumExpr(varExpr=SDQLInspector.replace_var(sum_el, old_dict, new_dict),
                       dictExpr=SDQLInspector.replace_var(sum_on, old_dict, new_dict),
                       bodyExpr=SDQLInspector.replace_var(sum_op, old_dict, new_dict),
                       isAssignmentSum=sum_assign)

    if type(sdql_obj) == IfExpr:
        if_cond = sdql_obj.condExpr
        if_then = sdql_obj.thenBodyExpr
        if_else = sdql_obj.elseBodyExpr

        return IfExpr(condExpr=SDQLInspector.replace_var(if_cond, old_dict, new_dict),
                      thenBodyExpr=SDQLInspector.replace_var(if_then, old_dict, new_dict),
                      elseBodyExpr=SDQLInspector.replace_var(if_else, old_dict, new_dict))

    if type(sdql_obj) == CompareExpr:
        cmp_symbol = sdql_obj.compareType
        cmp_left = sdql_obj.leftExpr
        cmp_right = sdql_obj.rightExpr

        return CompareExpr(compareType=cmp_symbol,
                           leftExpr=SDQLInspector.replace_var(cmp_left, old_dict, new_dict),
                           rightExpr=SDQLInspector.replace_var(cmp_right, old_dict, new_dict))

    if type(sdql_obj) == MulExpr:
        mul_el_1 = sdql_obj.op1Expr
        mul_el_2 = sdql_obj.op2Expr

        return MulExpr(op1Expr=SDQLInspector.replace_var(mul_el_1, old_dict, new_dict),
                       op2Expr=SDQLInspector.replace_var(mul_el_2, old_dict, new_dict))

    if type(sdql_obj) == AddExpr:
        add_el_1 = sdql_obj.op1Expr
        add_el_2 = sdql_obj.op2Expr

        return AddExpr(op1Expr=SDQLInspector.replace_var(add_el_1, old_dict, new_dict),
                       op2Expr=SDQLInspector.replace_var(add_el_2, old_dict, new_dict))

    if type(sdql_obj) == DivExpr:
        div_el_1 = sdql_obj.op1Expr
        div_el_2 = sdql_obj.op2Expr

        return DivExpr(op1Expr=SDQLInspector.replace_var(div_el_1, old_dict, new_dict),
                       op2Expr=SDQLInspector.replace_var(div_el_2, old_dict, new_dict))

    if type(sdql_obj) == SubExpr:
        sub_el_1 = sdql_obj.op1Expr
        sub_el_2 = sdql_obj.op2Expr

        return SubExpr(op1Expr=SDQLInspector.replace_var(sub_el_1, old_dict, new_dict),
                       op2Expr=SDQLInspector.replace_var(sub_el_2, old_dict, new_dict))

    if type(sdql_obj) == DicConsExpr:
        dict_items = sdql_obj.initialPairs

        new_items = []
        for i in dict_items:
            new_items.append((SDQLInspector.replace_var(i[0], old_dict, new_dict),
                              SDQLInspector.replace_var(i[1], old_dict, new_dict)))

        return DicConsExpr(new_items)

    if type(sdql_obj) == DicLookupExpr:
        the_dict = sdql_obj.dicExpr
        the_key = sdql_obj.keyExpr

        return DicLookupExpr(dicExpr=SDQLInspector.replace_var(the_dict, old_dict, new_dict),
                             keyExpr=SDQLInspector.replace_var(the_key, old_dict, new_dict))

    if type(sdql_obj) == RecConsExpr:
        rec_items = sdql_obj.initialPairs

        new_items = []
        for i in rec_items:
            new_items.append((SDQLInspector.replace_var(i[0], old_dict, new_dict),
                              SDQLInspector.replace_var(i[1], old_dict, new_dict)))

        return RecConsExpr(new_items)

    if type(sdql_obj) == RecAccessExpr:
        the_rec = sdql_obj.recExpr
        the_field = sdql_obj.name

        return RecAccessExpr(recExpr=SDQLInspector.replace_var(the_rec, old_dict, new_dict),
                             fieldName=the_field)

    if type(sdql_obj) == PairAccessExpr:
        dict_el = sdql_obj.pairExpr
        el_index = sdql_obj.index

        return PairAccessExpr(pairExpr=SDQLInspector.replace_var(dict_el, old_dict, new_dict),
                              index=SDQLInspector.replace_var(el_index, old_dict, new_dict))

    if type(sdql_obj) == ConcatExpr:
        rec1 = sdql_obj.rec1
        rec2 = sdql_obj.rec2

        return ConcatExpr(rec1=SDQLInspector.replace_var(rec1, old_dict, new_dict),
                          rec2=SDQLInspector.replace_var(rec2, old_dict, new_dict))

    if type(sdql_obj) == ExtFuncExpr:
        flag = sdql_obj.symbol
        arg1 = sdql_obj.inp1
        arg2 = sdql_obj.inp2
        arg3 = sdql_obj.inp3

        return ExtFuncExpr(symbol=flag,
                           inp1=SDQLInspector.replace_var(arg1, old_dict, new_dict),
                           inp2=SDQLInspector.replace_var(arg2, old_dict, new_dict),
                           inp3=SDQLInspector.replace_var(arg3, old_dict, new_dict))

    raise NotImplementedError


def findall_var(sdql_obj):
    all_var = []

    if type(sdql_obj) == VarExpr:
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

    if type(sdql_obj) == CompareExpr:
        cmp_left = sdql_obj.leftExpr
        cmp_right = sdql_obj.rightExpr

        all_var += SDQLInspector.findall_var(cmp_left)
        all_var += SDQLInspector.findall_var(cmp_right)

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

    if type(sdql_obj) == CompareExpr:
        cmp_left = sdql_obj.leftExpr
        cmp_right = sdql_obj.rightExpr

        all_const += SDQLInspector.findall_const(cmp_left)
        all_const += SDQLInspector.findall_const(cmp_right)

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
