from pysdql.core.dtypes.sdql_ir import *


class SDQLInspector:
    def __init__(self, ):
        pass

    @staticmethod
    def var_eq(var1: VarExpr, var2: VarExpr):
        if var1.name == var2.name:
            return True
        else:
            return False

    @staticmethod
    def const_eq(const1: ConstantExpr, const2: ConstantExpr):
        if const1.value == const2.value:
            return True
        else:
            return False

    @staticmethod
    def apply_all(sdql_obj, func, exclude=None, *args):
        if exclude is None:
            exclude = []

        if type(sdql_obj) in exclude:
            return sdql_obj

        elif type(sdql_obj) in (bool, int, float, str, ConstantExpr, VarExpr):
            return sdql_obj

        elif type(sdql_obj) == LetExpr:
            let_var = sdql_obj.varExpr
            let_val = sdql_obj.valExpr
            let_body = sdql_obj.bodyExpr

            return LetExpr(varExpr=func(let_var, *args),
                           valExpr=func(let_val, *args),
                           bodyExpr=func(let_body, *args))

        elif type(sdql_obj) == SumExpr:
            sum_el = sdql_obj.varExpr
            sum_on = sdql_obj.dictExpr
            sum_op = sdql_obj.bodyExpr
            sum_assign = sdql_obj.isAssignmentSum

            return SumExpr(varExpr=func(sum_el, *args),
                           dictExpr=func(sum_on, *args),
                           bodyExpr=func(sum_op, *args),
                           isAssignmentSum=sum_assign)

        elif type(sdql_obj) == IfExpr:
            if_cond = sdql_obj.condExpr
            if_then = sdql_obj.thenBodyExpr
            if_else = sdql_obj.elseBodyExpr

            return IfExpr(condExpr=func(if_cond, *args),
                          thenBodyExpr=func(if_then, *args),
                          elseBodyExpr=func(if_else, *args))

        elif type(sdql_obj) == CompareExpr:
            cmp_symbol = sdql_obj.compareType
            cmp_left = sdql_obj.leftExpr
            cmp_right = sdql_obj.rightExpr

            return CompareExpr(compareType=cmp_symbol,
                               leftExpr=func(cmp_left, *args),
                               rightExpr=func(cmp_right, *args))

        elif type(sdql_obj) == MulExpr:
            mul_el_1 = sdql_obj.op1Expr
            mul_el_2 = sdql_obj.op2Expr

            return MulExpr(op1Expr=func(mul_el_1, *args),
                           op2Expr=func(mul_el_2, *args))

        elif type(sdql_obj) == AddExpr:
            add_el_1 = sdql_obj.op1Expr
            add_el_2 = sdql_obj.op2Expr

            return AddExpr(op1Expr=func(add_el_1, *args),
                           op2Expr=func(add_el_2, *args))

        elif type(sdql_obj) == DivExpr:
            div_el_1 = sdql_obj.op1Expr
            div_el_2 = sdql_obj.op2Expr

            return DivExpr(op1Expr=func(div_el_1, *args),
                           op2Expr=func(div_el_2, *args))

        elif type(sdql_obj) == SubExpr:
            sub_el_1 = sdql_obj.op1Expr
            sub_el_2 = sdql_obj.op2Expr

            return SubExpr(op1Expr=func(sub_el_1, *args),
                           op2Expr=func(sub_el_2, *args))

        elif type(sdql_obj) == DicConsExpr:
            dict_items = sdql_obj.initialPairs

            new_items = []
            for i in dict_items:
                new_items.append((func(i[0], *args),
                                  func(i[1], *args)))

            return DicConsExpr(new_items)

        elif type(sdql_obj) == DicLookupExpr:
            the_dict = sdql_obj.dicExpr
            the_key = sdql_obj.keyExpr

            return DicLookupExpr(dicExpr=func(the_dict, *args),
                                 keyExpr=func(the_key, *args))

        elif type(sdql_obj) == RecConsExpr:
            rec_items = sdql_obj.initialPairs

            new_items = []
            for i in rec_items:
                new_items.append((func(i[0], *args),
                                  func(i[1], *args)))

            return RecConsExpr(new_items)

        elif type(sdql_obj) == RecAccessExpr:
            the_rec = sdql_obj.recExpr
            the_field = sdql_obj.name

            return RecAccessExpr(recExpr=func(the_rec, *args),
                                 fieldName=the_field)

        elif type(sdql_obj) == PairAccessExpr:
            dict_el = sdql_obj.pairExpr
            el_index = sdql_obj.index

            return PairAccessExpr(pairExpr=func(dict_el, *args),
                                  index=func(el_index, *args))

        elif type(sdql_obj) == ConcatExpr:
            rec1 = sdql_obj.rec1
            rec2 = sdql_obj.rec2

            return ConcatExpr(rec1=func(rec1, *args),
                              rec2=func(rec2, *args))

        elif type(sdql_obj) == ExtFuncExpr:
            flag = sdql_obj.symbol
            arg1 = sdql_obj.inp1
            arg2 = sdql_obj.inp2
            arg3 = sdql_obj.inp3

            return ExtFuncExpr(symbol=flag,
                               inp1=func(arg1, *args),
                               inp2=func(arg2, *args),
                               inp3=func(arg3, *args))

        raise NotImplementedError

    @staticmethod
    def replace_cond(sdql_obj, cond):
        return IfExpr(condExpr=cond,
                      thenBodyExpr=sdql_obj.thenBodyExpr,
                      elseBodyExpr=sdql_obj.elseBodyExpr)

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
        if isinstance(sdql_obj, IfExpr):
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
        if isinstance(sdql_obj, (DicConsExpr,
                                 RecConsExpr)):
            return IfExpr(condExpr=cond,
                          thenBodyExpr=sdql_obj,
                          elseBodyExpr=ConstantExpr(None))
        if isinstance(sdql_obj, (RecAccessExpr,
                                 MulExpr,
                                 AddExpr,
                                 DivExpr,
                                 SubExpr)):
            return IfExpr(condExpr=cond,
                          thenBodyExpr=sdql_obj,
                          elseBodyExpr=ConstantExpr(0.0))

        raise TypeError(f'Unsupported Type: {type(sdql_obj)}. '
                        f'Must annotate the default value if condition is false.')

    @staticmethod
    def replace_var(sdql_obj, old_var, new_var):
        if type(sdql_obj) == VarExpr:
            if SDQLInspector.var_eq(sdql_obj, old_var):
                return new_var
            else:
                return sdql_obj
        else:
            return SDQLInspector.apply_all(sdql_obj,
                                           SDQLInspector.replace_var,
                                           old_var,
                                           new_var)

    @staticmethod
    def findall_bindings(sdql_obj):
        bindings = []

        if type(sdql_obj) == LetExpr:
            let_var = sdql_obj.varExpr
            let_val = sdql_obj.valExpr
            let_body = sdql_obj.bodyExpr

            bindings.append((let_var, let_val))

            bindings += SDQLInspector.gather_all(let_body,
                                                 SDQLInspector.findall_bindings)

        return bindings

    @staticmethod
    def locate_iter(sdql_obj, target_dict):
        bindings = SDQLInspector.findall_bindings(sdql_obj)

        for the_var, the_val in bindings:
            if type(the_val) == SumExpr:
                sum_on = sdql_obj.dictExpr
                if SDQLInspector.var_eq(sum_on, target_dict):
                    return the_var, the_val
        return None, None

    @staticmethod
    def gather_all(sdql_obj, func, exclude=None, *args):
        if exclude is None:
            exclude = []

        all_list = []

        if type(sdql_obj) in exclude:
            return []

        elif type(sdql_obj) in (bool, int, float, str, ConstantExpr, VarExpr):
            return []

        elif type(sdql_obj) == LetExpr:
            let_var = sdql_obj.varExpr
            let_val = sdql_obj.valExpr
            let_body = sdql_obj.bodyExpr

            all_list += func(let_var, *args)
            all_list += func(let_val, *args)
            all_list += func(let_body, *args)

        elif type(sdql_obj) == SumExpr:
            sum_el = sdql_obj.varExpr
            sum_on = sdql_obj.dictExpr
            sum_op = sdql_obj.bodyExpr

            all_list += func(sum_el, *args)
            all_list += func(sum_on, *args)
            all_list += func(sum_op, *args)

        elif type(sdql_obj) == IfExpr:
            if_cond = sdql_obj.condExpr
            if_then = sdql_obj.thenBodyExpr
            if_else = sdql_obj.elseBodyExpr

            all_list += func(if_cond, *args)
            all_list += func(if_then, *args)
            all_list += func(if_else, *args)

        elif type(sdql_obj) == CompareExpr:
            cmp_left = sdql_obj.leftExpr
            cmp_right = sdql_obj.rightExpr

            all_list += func(cmp_left, *args)
            all_list += func(cmp_right, *args)

        elif type(sdql_obj) == MulExpr:
            mul_el_1 = sdql_obj.op1Expr
            mul_el_2 = sdql_obj.op2Expr

            all_list += func(mul_el_1, *args)
            all_list += func(mul_el_2, *args)

        elif type(sdql_obj) == AddExpr:
            add_el_1 = sdql_obj.op1Expr
            add_el_2 = sdql_obj.op2Expr

            all_list += func(add_el_1, *args)
            all_list += func(add_el_2, *args)

        elif type(sdql_obj) == DivExpr:
            div_el_1 = sdql_obj.op1Expr
            div_el_2 = sdql_obj.op2Expr

            all_list += func(div_el_1, *args)
            all_list += func(div_el_2, *args)

        elif type(sdql_obj) == SubExpr:
            sub_el_1 = sdql_obj.op1Expr
            sub_el_2 = sdql_obj.op2Expr

            all_list += func(sub_el_1, *args)
            all_list += func(sub_el_2, *args)

        elif type(sdql_obj) == DicConsExpr:
            dict_items = sdql_obj.initialPairs

            for i in dict_items:
                all_list += func(i, *args)

        elif type(sdql_obj) == DicLookupExpr:
            the_dict = sdql_obj.dicExpr
            the_key = sdql_obj.keyExpr

            all_list += func(the_dict, *args)
            all_list += func(the_key, *args)

        elif type(sdql_obj) == RecConsExpr:
            rec_items = sdql_obj.initialPairs

            for i in rec_items:
                all_list += func(i, *args)

        elif type(sdql_obj) == RecAccessExpr:
            the_rec = sdql_obj.recExpr

            all_list += func(the_rec, *args)

        elif type(sdql_obj) == PairAccessExpr:
            dict_el = sdql_obj.pairExpr

            all_list += func(dict_el, *args)

        elif type(sdql_obj) == ConcatExpr:
            rec1 = sdql_obj.rec1
            rec2 = sdql_obj.rec2

            all_list += func(rec1, *args)
            all_list += func(rec2, *args)

        elif type(sdql_obj) == ExtFuncExpr:
            arg1 = sdql_obj.inp1
            arg2 = sdql_obj.inp2
            arg3 = sdql_obj.inp3

            all_list += func(arg1, *args)
            all_list += func(arg2, *args)
            all_list += func(arg3, *args)

        elif type(sdql_obj) == tuple:
            for i in sdql_obj:
                all_list += func(i, *args)
        else:
            print(sdql_obj)
            raise NotImplementedError(f'Unsupport type {type(sdql_obj)}')

        return all_list

    @staticmethod
    def findall_const(sdql_obj):
        all_var = []

        if type(sdql_obj) == ConstantExpr:
            all_var.append(sdql_obj)
        else:
            all_var += SDQLInspector.gather_all(sdql_obj, SDQLInspector.findall_const)

        return all_var

    @staticmethod
    def findall_var(sdql_obj):
        all_var = []

        if type(sdql_obj) == VarExpr:
            all_var.append(sdql_obj)
        else:
            all_var += SDQLInspector.gather_all(sdql_obj, SDQLInspector.findall_var)

        return all_var

    @staticmethod
    def finall_cols(sdql_obj):
        all_cols = []

        if isinstance(sdql_obj, RecAccessExpr):
            all_cols.append(sdql_obj.name)
        else:
            pass

        return all_cols

    @staticmethod
    def find_cols(sdql_obj):
        cols = []

        if isinstance(sdql_obj, RecAccessExpr):
            cols.append(sdql_obj.name)
        else:
            cols += SDQLInspector.gather_all(sdql_obj, SDQLInspector.find_cols)

        return cols

    @staticmethod
    def split_conds(sdql_obj):
        pass

    @staticmethod
    def add_binding(bind1, bind2) -> LetExpr:
        result = LetExpr(bind1.varExpr,
                         bind1.valExpr,
                         LetExpr(bind2.varExpr,
                                 bind2.valExpr,
                                 bind2.bodyExpr))

        return result

    @staticmethod
    def split_bindings(sdql_obj) -> list:
        result = []
        if isinstance(sdql_obj, LetExpr):
            var_expr = sdql_obj.varExpr
            val_expr = sdql_obj.valExpr
            body_expr = sdql_obj.bodyExpr

            result.append(LetExpr(varExpr=var_expr,
                                  valExpr=val_expr,
                                  bodyExpr=ConstantExpr(True)))

            if isinstance(body_expr, LetExpr):
                result += SDQLInspector.split_bindings(body_expr)

        return result

    @staticmethod
    def rename_last_binding(sdql_obj, new_name):
        all_bindings = SDQLInspector.split_bindings(sdql_obj)
        all_bindings[-1] = LetExpr(VarExpr(new_name),
                                   all_bindings[-1].valExpr,
                                   all_bindings[-1].bodyExpr)
        all_bindings.append(LetExpr(VarExpr('results'),
                                    VarExpr(new_name),
                                    ConstantExpr(True)))
        return SDQLInspector.concat_bindings(all_bindings)

    @staticmethod
    def replace_binding(old_obj: LetExpr, new_obj: Expr, mode='body'):
        if mode == 'body':
            if isinstance(old_obj.bodyExpr, LetExpr):
                return LetExpr(varExpr=old_obj.varExpr,
                               valExpr=old_obj.valExpr,
                               bodyExpr=SDQLInspector.replace_binding(old_obj.bodyExpr, new_obj))
            else:
                return LetExpr(varExpr=old_obj.varExpr,
                               valExpr=old_obj.valExpr,
                               bodyExpr=new_obj)

    @staticmethod
    def remove_dup_bindings(bindings: list):
        result = []
        unique_names = []
        for b in bindings:
            if isinstance(b, LetExpr):
                if not any([b.varExpr.name == r.varExpr.name for r in result]):
                    result.append(b)
                    unique_names.append(b.varExpr.name)
        return result

    @staticmethod
    def concat_bindings(bindings: list, drop_dup=True) -> LetExpr:
        flatten = []
        for b in bindings:
            flatten += SDQLInspector.split_bindings(b)

        if drop_dup:
            flatten = SDQLInspector.remove_dup_bindings(flatten)

        result = None

        if not len(flatten) >= 2:
            print(flatten)
            raise ValueError()
        else:
            first_let = flatten[0]
            second_let = flatten[1]

            if isinstance(first_let, LetExpr) and isinstance(second_let, LetExpr):
                result = LetExpr(first_let.varExpr,
                                 first_let.valExpr,
                                 LetExpr(second_let.varExpr,
                                         second_let.valExpr,
                                         second_let.bodyExpr))

                if len(bindings) >= 3:
                    for i in range(2, len(flatten)):
                        result = SDQLInspector.replace_binding(result, flatten[i])

        return result

    @staticmethod
    def equal_expr(expr1, expr2):
        if type(expr1) != type(expr2):
            return False

        if not isinstance(expr1, Expr):
            raise TypeError(f'Exepcting an sdql expr.')

        return False

    @staticmethod
    def find_last_structure(sdql_obj: LetExpr):
        all_bindings = SDQLInspector.split_bindings(sdql_obj)

        last_binding = all_bindings[-1]

        if isinstance(last_binding, LetExpr):
            bind_value = last_binding.valExpr

            if isinstance(bind_value, SumExpr):
                bind_body = bind_value.bodyExpr

                while isinstance(bind_body, IfExpr):
                    bind_body = bind_body.thenBodyExpr

                if isinstance(bind_body, DicConsExpr):
                    this_key = bind_body.initialPairs[0][0]
                    this_val = bind_body.initialPairs[0][1]

                    if isinstance(this_key, RecAccessExpr):
                        key_out = this_key.name
                    elif isinstance(this_key, RecConsExpr):
                        key_list = []
                        for rec_key in this_key.initialPairs:
                            key_list.append(rec_key[0])
                        key_out = tuple(key_list)
                    else:
                        raise NotImplementedError

                    if isinstance(this_val, RecAccessExpr):
                        val_out = this_val.name
                    elif isinstance(this_val, RecConsExpr):
                        val_list = []
                        for rec_val in this_val.initialPairs:
                            val_list.append(rec_val[0])
                        val_out = tuple(val_list)
                    else:
                        raise NotImplementedError

                    return {key_out: val_out}
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError(f'Unexpected last binding value {bind_value}')
        else:
            raise ValueError('Last binding is not a LetExpr')

    @staticmethod
    def get_last_binding_name(bindings):
        return bindings[-1].varExpr.name

    @staticmethod
    def findall_non_null(sdql_obj):
        all_non_null = []

        if isinstance(sdql_obj, DicLookupExpr):
            all_non_null.append(sdql_obj)

            if isinstance(sdql_obj.keyExpr, RecAccessExpr):
                if isinstance(sdql_obj.keyExpr.recExpr, DicLookupExpr):
                    all_non_null += SDQLInspector.gather_all(sdql_obj.keyExpr.recExpr, SDQLInspector.findall_non_null)
        else:
            all_non_null += SDQLInspector.gather_all(sdql_obj, SDQLInspector.findall_non_null)

        cleaned_non_null = []
        dup_non_null_strs = []

        for i in all_non_null:
            if str(i) not in dup_non_null_strs:
                dup_non_null_strs.append(str(i))
                cleaned_non_null.append(i)

        return cleaned_non_null

    @staticmethod
    def replace_access(sdql_obj, to_rec):
        if isinstance(sdql_obj, ConstantExpr):
            return sdql_obj

        if isinstance(sdql_obj, VarExpr):
            return sdql_obj

        if isinstance(sdql_obj, RecAccessExpr):
            field = sdql_obj.name

            return RecAccessExpr(to_rec, field)

        if isinstance(sdql_obj, AddExpr):
            el_1 = SDQLInspector.replace_access(sdql_obj.op1Expr, to_rec)
            el_2 = SDQLInspector.replace_access(sdql_obj.op2Expr, to_rec)

            return AddExpr(el_1, el_2)

        if isinstance(sdql_obj, SubExpr):
            el_1 = SDQLInspector.replace_access(sdql_obj.op1Expr, to_rec)
            el_2 = SDQLInspector.replace_access(sdql_obj.op2Expr, to_rec)

            return SubExpr(el_1, el_2)

        if isinstance(sdql_obj, MulExpr):
            el_1 = SDQLInspector.replace_access(sdql_obj.op1Expr, to_rec)
            el_2 = SDQLInspector.replace_access(sdql_obj.op2Expr, to_rec)

            return MulExpr(el_1, el_2)

        if isinstance(sdql_obj, DivExpr):
            el_1 = SDQLInspector.replace_access(sdql_obj.op1Expr, to_rec)
            el_2 = SDQLInspector.replace_access(sdql_obj.op2Expr, to_rec)

            return DivExpr(el_1, el_2)

        if isinstance(sdql_obj, IfExpr):
            el_cond = SDQLInspector.replace_access(sdql_obj.condExpr, to_rec)
            el_then = SDQLInspector.replace_access(sdql_obj.thenBodyExpr, to_rec)
            el_else = SDQLInspector.replace_access(sdql_obj.elseBodyExpr, to_rec)

            return IfExpr(el_cond, el_then, el_else)

        if isinstance(sdql_obj, ExtFuncExpr):
            el1 = SDQLInspector.replace_access(sdql_obj.inp1, to_rec)
            el2 = SDQLInspector.replace_access(sdql_obj.inp2, to_rec)
            el3 = SDQLInspector.replace_access(sdql_obj.inp3, to_rec)

            return ExtFuncExpr(sdql_obj.symbol, el1, el2, el3)

        if isinstance(sdql_obj, CompareExpr):
            if sdql_obj.compareType == CompareSymbol.NE and isinstance(sdql_obj.leftExpr, DicLookupExpr) and isinstance(sdql_obj.rightExpr, ConstantExpr):
                if sdql_obj.rightExpr.value is None:
                    return ConstantExpr(True)
            else:
                left = SDQLInspector.replace_access(sdql_obj.leftExpr, to_rec)
                right = SDQLInspector.replace_access(sdql_obj.rightExpr, to_rec)

                return CompareExpr(sdql_obj.compareType, left, right)

        return sdql_obj

    @staticmethod
    def replace_field(sdql_obj, inplace=True, mapper=None):
        if isinstance(sdql_obj, ConstantExpr):
            return sdql_obj

        if isinstance(sdql_obj, RecAccessExpr):
            field = sdql_obj.name

            if mapper:
                if field in mapper.keys():
                    if inplace:
                        return mapper[field]
                    else:
                        return RecAccessExpr(mapper[field], field)
                else:
                    return sdql_obj

                    # raise IndexError(f'{field} is not in mapper keys {mapper.keys()}')
            else:
                raise IndexError(f'mapper is None')

        if isinstance(sdql_obj, AddExpr):
            el_1 = SDQLInspector.replace_field(sdql_obj.op1Expr, inplace, mapper)
            el_2 = SDQLInspector.replace_field(sdql_obj.op2Expr, inplace, mapper)

            return AddExpr(el_1, el_2)

        if isinstance(sdql_obj, SubExpr):
            el_1 = SDQLInspector.replace_field(sdql_obj.op1Expr, inplace, mapper)
            el_2 = SDQLInspector.replace_field(sdql_obj.op2Expr, inplace, mapper)

            return SubExpr(el_1, el_2)

        if isinstance(sdql_obj, MulExpr):
            el_1 = SDQLInspector.replace_field(sdql_obj.op1Expr, inplace, mapper)
            el_2 = SDQLInspector.replace_field(sdql_obj.op2Expr, inplace, mapper)

            return MulExpr(el_1, el_2)

        if isinstance(sdql_obj, DivExpr):
            el_1 = SDQLInspector.replace_field(sdql_obj.op1Expr, inplace, mapper)
            el_2 = SDQLInspector.replace_field(sdql_obj.op2Expr, inplace, mapper)

            return DivExpr(el_1, el_2)

        if isinstance(sdql_obj, IfExpr):
            el_cond = SDQLInspector.replace_field(sdql_obj.condExpr, inplace, mapper)
            el_then = SDQLInspector.replace_field(sdql_obj.thenBodyExpr, inplace, mapper)
            el_else = SDQLInspector.replace_field(sdql_obj.elseBodyExpr, inplace, mapper)

            return IfExpr(el_cond, el_then, el_else)

        if isinstance(sdql_obj, ExtFuncExpr):
            return sdql_obj

        if isinstance(sdql_obj, CompareExpr):
            return sdql_obj

    @staticmethod
    def check_equal_expr(op1, op2):
        if str(op1) == str(op2):
            return True
        else:
            return False