from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.EnumUtil import AggrType
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.util.df_retriever import Retriever
from pysdql.core.dtypes.sdql_ir import *


class AggrFrame:
    def __init__(self, aggr_on):
        self.aggr_on = aggr_on

        self.vname_aggr = f'{aggr_on.name}_aggr'
        self.var_aggr = VarExpr(self.vname_aggr)
        self.vname_x_aggr = f'x_{self.vname_aggr}'
        self.var_x_aggr = VarExpr(self.vname_x_aggr)

    @property
    def retriever(self) -> Retriever:
        return self.aggr_on.get_retriever()

    @property
    def sdql_ir(self):
        # Q6

        aggr_info = self.retriever.find_aggr()

        aggr_dict = aggr_info.aggr_op

        cond = self.retriever.find_cond_before(AggrExpr)
        col_ins = self.retriever.find_col_ins_before(AggrExpr)

        if len(aggr_dict.keys()) == 1:
            '''
            Aggregation as a single value
            Then format to a singleton dictionary
            '''
            dict_val = list(aggr_dict.items())[0][1]

            if isinstance(dict_val, RecAccessExpr):
                # (, 'sum')
                dict_val_name = dict_val.name

                if dict_val_name in self.aggr_on.columns:
                    aggr_body = self.aggr_on.key_access(dict_val_name)
                else:
                    if dict_val_name in col_ins.keys():
                        aggr_body = col_ins[dict_val_name].replace(self.aggr_on.iter_el.key)
                    else:
                        raise IndexError(f'Cannot find column {dict_val_name} in {self.aggr_on.columns}')

                if cond:
                    aggr_body = IfExpr(condExpr=cond.sdql_ir,
                                       thenBodyExpr=aggr_body,
                                       elseBodyExpr=ConstantExpr(0.0))
            elif isinstance(dict_val, ConstantExpr):
                # (, 'count')
                aggr_body = dict_val

                if cond:
                    aggr_body = IfExpr(condExpr=cond.sdql_ir,
                                       thenBodyExpr=aggr_body,
                                       elseBodyExpr=ConstantExpr(0))
            elif isinstance(dict_val, (AddExpr,
                                       SubExpr,
                                       MulExpr,
                                       DivExpr,
                                       )):
                aggr_body = dict_val
            else:
                raise NotImplementedError(dict_val)
        else:
            '''
            Aggregation as a single record
            Then format to a singleton dictionary
            '''
            aggr_tuples = []
            for k in aggr_dict.keys():
                v = aggr_dict[k]
                if isinstance(v, RecAccessExpr):
                    # (, 'sum')
                    v_name = v.name
                    if v_name in self.aggr_on.columns:
                        aggr_tuples.append((k, self.aggr_on.key_access(v_name)))
                    else:
                        if v_name in col_ins.keys():
                            aggr_tuples.append((k,
                                                col_ins[v_name].replace(self.aggr_on.iter_el.key)))
                        else:
                            raise IndexError(f'Cannot find column {k} in {self.aggr_on.columns}')
                elif isinstance(v, ConstantExpr):
                    # (, 'count')
                    aggr_tuples.append((k, v))
                else:
                    raise NotImplementedError

            aggr_body = RecConsExpr(aggr_tuples)

            if cond:
                aggr_body = IfExpr(condExpr=cond.sdql_ir,
                                   thenBodyExpr=aggr_body,
                                   elseBodyExpr=ConstantExpr(None))

        aggr_sum_expr = SumExpr(varExpr=self.aggr_on.iter_el.sdql_ir,
                                dictExpr=self.aggr_on.var_expr,
                                bodyExpr=aggr_body,
                                isAssignmentSum=False)

        vname_aggr = f'{self.aggr_on.name}_aggr'
        var_aggr = VarExpr(vname_aggr)
        self.aggr_on.add_context_variable(vname_aggr,
                                          var_aggr)

        aggr_let_expr = LetExpr(varExpr=var_aggr,
                                valExpr=aggr_sum_expr,
                                bodyExpr=ConstantExpr(True))

        if len(aggr_dict.keys()) == 1:
            dict_key = list(aggr_dict.items())[0][0]

            if aggr_info.aggr_type == AggrType.Dict:
                format_op = DicConsExpr([(RecConsExpr([(dict_key, var_aggr)]), ConstantExpr(True))])
            elif aggr_info.aggr_type == AggrType.Scalar:
                format_op = var_aggr
            else:
                raise NotImplementedError
        else:
            format_op = DicConsExpr([(var_aggr, ConstantExpr(True))])

        vname_res = f'results'
        var_res = VarExpr(vname_res)
        self.aggr_on.add_context_variable(vname_res,
                                          var_res)

        form_let_expr = LetExpr(varExpr=var_res,
                                valExpr=format_op,
                                bodyExpr=ConstantExpr(True))

        return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])

    def get_aggr_expr(self, next_op, as_part=False) -> LetExpr:
        if as_part:
            aggr_info = self.retriever.find_aggr()

            aggr_dict = aggr_info.aggr_op

            cond = self.retriever.find_cond_before(AggrExpr)
            col_ins = self.retriever.find_col_ins_before(AggrExpr)

            if len(aggr_dict.keys()) == 1:
                '''
                Aggregation as a single value
                Then format to a singleton dictionary
                '''
                dict_val = list(aggr_dict.items())[0][1]

                if isinstance(dict_val, RecAccessExpr):
                    # (, 'sum')
                    dict_val_name = dict_val.name

                    if dict_val_name in self.aggr_on.columns:
                        aggr_body = self.aggr_on.key_access(dict_val_name)
                    else:
                        if dict_val_name in col_ins.keys():
                            aggr_body = col_ins[dict_val_name].replace(self.aggr_on.iter_el.key)
                        else:
                            raise IndexError(f'Cannot find column {dict_val_name} in {self.aggr_on.columns}')

                    if cond:
                        aggr_body = IfExpr(condExpr=cond.sdql_ir,
                                           thenBodyExpr=aggr_body,
                                           elseBodyExpr=ConstantExpr(0.0))
                elif isinstance(dict_val, ConstantExpr):
                    # (, 'count')
                    aggr_body = dict_val

                    if cond:
                        aggr_body = IfExpr(condExpr=cond.sdql_ir,
                                           thenBodyExpr=aggr_body,
                                           elseBodyExpr=ConstantExpr(0))
                else:
                    raise NotImplementedError
            else:
                '''
                Aggregation as a single record
                Then format to a singleton dictionary
                '''
                aggr_tuples = []
                for k in aggr_dict.keys():
                    v = aggr_dict[k]
                    if isinstance(v, RecAccessExpr):
                        # (, 'sum')
                        v_name = v.name
                        if v_name in self.aggr_on.columns:
                            aggr_tuples.append((k, self.aggr_on.key_access(v_name)))
                        else:
                            if v_name in col_ins.keys():
                                aggr_tuples.append((k,
                                                    col_ins[v_name].replace(self.aggr_on.iter_el.key)))
                            else:
                                raise IndexError(f'Cannot find column {k} in {self.aggr_on.columns}')
                    elif isinstance(v, ConstantExpr):
                        # (, 'count')
                        aggr_tuples.append((k, v))
                    else:
                        raise NotImplementedError

                aggr_body = RecConsExpr(aggr_tuples)

                if cond:
                    aggr_body = IfExpr(condExpr=cond.sdql_ir,
                                       thenBodyExpr=aggr_body,
                                       elseBodyExpr=ConstantExpr(None))

            aggr_sum_expr = SumExpr(varExpr=self.aggr_on.iter_el.sdql_ir,
                                    dictExpr=self.aggr_on.var_expr,
                                    bodyExpr=aggr_body,
                                    isAssignmentSum=False)

            self.aggr_on.add_context_variable(self.vname_aggr,
                                              self.var_aggr)

            aggr_let_expr = LetExpr(varExpr=self.var_aggr,
                                    valExpr=aggr_sum_expr,
                                    bodyExpr=ConstantExpr(True))
        else:
            aggr_let_expr = self.sdql_ir

        if next_op:
            return LetExpr(varExpr=aggr_let_expr.varExpr,
                           valExpr=aggr_let_expr.valExpr,
                           bodyExpr=next_op)
        else:
            return LetExpr(varExpr=aggr_let_expr.varExpr,
                           valExpr=aggr_let_expr.valExpr,
                           bodyExpr=ConstantExpr(True))