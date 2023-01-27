from pysdql.core.dtypes.GroupByAgg import GroupbyAggrExpr
from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.dtypes.sdql_ir import *


class GroupbyAggrFrame:
    def __init__(self, aggr_on):
        self.aggr_on = aggr_on

        self.vname_aggr = f'{aggr_on.name}_aggr'
        self.var_aggr = VarExpr(self.vname_aggr)
        self.vname_x_aggr = f'x_{self.vname_aggr}'
        self.var_x_aggr = VarExpr(self.vname_x_aggr)

    @property
    def retriever(self):
        return self.aggr_on.retriever

    @property
    def sdql_ir(self) -> LetExpr:
        # Q1
        # Q4
        groupby_aggr_info = self.retriever.find_groupby_aggr()

        aggr_dict = groupby_aggr_info.aggr_dict
        groupby_cols = groupby_aggr_info.groupby_cols

        cond = self.retriever.find_cond_before(GroupbyAggrExpr)
        col_ins = self.retriever.find_col_ins_before(GroupbyAggrExpr)
        isin_expr = self.retriever.find_isin_before(GroupbyAggrExpr)

        if len(groupby_cols) == 0:
            raise ValueError()
        elif len(groupby_cols) == 1:
            dict_key_ir = self.aggr_on.key_access(groupby_cols[0])
        else:
            key_tuples = []

            for c in groupby_cols:
                key_tuples.append((c, self.aggr_on.key_access(c)))

            dict_key_ir = RecConsExpr(key_tuples)

        if len(aggr_dict.keys()) == 0:
            raise ValueError()
        elif len(aggr_dict.keys()) == 1:
            '''
            Aggregation as a single value
            Then format to a singleton dictionary
            '''

            dict_val = list(aggr_dict.items())[0][1]

            if isinstance(dict_val, RecAccessExpr):
                # (, 'sum')
                dict_val_name = dict_val.name

                if dict_val_name in self.aggr_on.columns:
                    aggr_body = DicConsExpr([(dict_key_ir,
                                              self.aggr_on.key_access(dict_val_name))])
                else:
                    if dict_val_name in col_ins.keys():
                        aggr_body = DicConsExpr([(dict_key_ir,
                                                  col_ins[dict_val_name].replace(self.aggr_on.iter_el.key))])
                    else:
                        raise IndexError(f'Cannot find column {dict_val_name} in {self.aggr_on.columns}')
            elif isinstance(dict_val, ConstantExpr):
                # (, 'count')
                aggr_body = DicConsExpr([(dict_key_ir,
                                          dict_val)])
            else:
                raise NotImplementedError
        else:
            '''
            Aggregation as a single record
            Then format to a singleton dictionary
            '''
            val_tuples = []
            for k in aggr_dict.keys():
                v = aggr_dict[k]

                if isinstance(v, RecAccessExpr):
                    # (, 'sum')
                    v_name = v.name

                    if v_name in self.aggr_on.columns:
                        val_tuples.append((k, self.aggr_on.key_access(v_name)))
                    else:
                        if v_name in col_ins.keys():
                            val_tuples.append((k,
                                               col_ins[v_name].replace(self.aggr_on.iter_el.key)))
                        else:
                            raise IndexError(f'Cannot find column {v_name} in {self.aggr_on.columns}')
                elif isinstance(v, ConstantExpr):
                    # (, 'count')
                    val_tuples.append((k, v))
                else:
                    raise NotImplementedError

            aggr_body = DicConsExpr([(dict_key_ir, RecConsExpr(val_tuples))])

        if cond:
            aggr_body = IfExpr(condExpr=cond,
                               thenBodyExpr=aggr_body,
                               elseBodyExpr=ConstantExpr(None))

        if isin_expr:
            aggr_body = SDQLInspector.add_cond(aggr_body,
                                               isin_expr.get_as_cond(),
                                               'inner')

        aggr_sum_expr = SumExpr(varExpr=self.aggr_on.iter_el.sdql_ir,
                                dictExpr=self.aggr_on.var_expr,
                                bodyExpr=aggr_body,
                                isAssignmentSum=False)

        self.aggr_on.add_context_variable(self.vname_aggr,
                                          self.var_aggr)

        aggr_let_expr = LetExpr(varExpr=self.var_aggr,
                                valExpr=aggr_sum_expr,
                                bodyExpr=ConstantExpr(True))

        self.aggr_on.add_context_variable(self.vname_x_aggr,
                                          self.var_x_aggr)

        # aggr = {? : scalar}
        if len(aggr_dict.keys()) == 1:
            dict_key = list(aggr_dict.items())[0][0]

            format_key_tuples = []

            # aggr = {scalar : scalar}
            if len(groupby_cols) == 1:
                format_key_tuples.append((groupby_cols[0],
                                          PairAccessExpr(self.var_x_aggr, 0)))
            # aggr = {record : scalar}
            else:
                for c in groupby_cols:
                    format_key_tuples.append((c, RecAccessExpr(PairAccessExpr(self.var_x_aggr, 0), c)))

            format_key_tuples.append((dict_key, PairAccessExpr(self.var_x_aggr, 1)))

            format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                      ConstantExpr(True))])
        # aggr = {? : record}
        else:
            # aggr = {scalar: record}
            if len(groupby_cols) == 1:
                format_key_tuples = [(groupby_cols[0],
                                      PairAccessExpr(self.var_x_aggr, 0))]

                for k in aggr_dict.keys():
                    format_key_tuples.append((k,
                                              RecAccessExpr(PairAccessExpr(self.var_x_aggr,
                                                                           1),
                                                            k)))

                format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                          ConstantExpr(True))])
            else:
                # aggr = {record : record}
                format_op = DicConsExpr([(ConcatExpr(PairAccessExpr(self.var_x_aggr, 0),
                                                     PairAccessExpr(self.var_x_aggr, 1)),
                                          ConstantExpr(True))])

        format_sum = SumExpr(varExpr=self.var_x_aggr,
                             dictExpr=self.var_aggr,
                             bodyExpr=format_op,
                             isAssignmentSum=True)

        vname_res = f'results'
        var_res = VarExpr(vname_res)
        self.aggr_on.add_context_variable(vname_res,
                                          var_res)

        form_let_expr = LetExpr(varExpr=var_res,
                                valExpr=format_sum,
                                bodyExpr=ConstantExpr(True))

        if isin_expr:
            isin_let_expr = isin_expr.get_as_part()
            return SDQLInspector.concat_bindings([isin_let_expr, aggr_let_expr, form_let_expr])
        else:
            return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])

    def get_groupby_aggr_expr(self, next_op) -> LetExpr:
        let_expr = self.sdql_ir
        if next_op:
            return LetExpr(varExpr=let_expr.varExpr,
                           valExpr=let_expr.valExpr,
                           bodyExpr=next_op)
        else:
            return LetExpr(varExpr=let_expr.varExpr,
                           valExpr=let_expr.valExpr,
                           bodyExpr=ConstantExpr(True))
