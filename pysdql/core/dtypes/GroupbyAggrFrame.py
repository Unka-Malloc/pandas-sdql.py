from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.GroupbyAggrExpr import GroupbyAggrExpr
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
        # Q22
        groupby_aggr_info = self.retriever.find_groupby_aggr()

        aggr_dict = groupby_aggr_info.aggr_dict
        origin_dict = groupby_aggr_info.origin_dict
        groupby_cols = groupby_aggr_info.groupby_cols

        cond = self.retriever.find_cond_before(GroupbyAggrExpr)
        col_ins = self.retriever.find_col_ins_before(GroupbyAggrExpr)
        isin_expr = self.retriever.find_isin_before(GroupbyAggrExpr)

        global_count_name = 'global_count'

        raw_cols_count = []

        cols_sum = []
        cols_mean = []
        key_mean = []
        mean_mapper = {}

        for k in origin_dict.keys():
            if origin_dict[k][1] == 'sum':
                cols_sum.append(origin_dict[k][0])
            if origin_dict[k][1] == 'mean':
                mean_mapper[f'{k}_sum_for_mean'] = k
                mean_mapper[f'{k}_count_for_mean'] = k
                cols_mean.append(origin_dict[k][0])
                key_mean.append(k)
            if origin_dict[k][1] == 'count':
                raw_cols_count.append(origin_dict[k][0])

        cols_to_go = list(set(i for i in cols_mean if i in cols_sum))

        index_mapper = {}
        for k in origin_dict.keys():
            if origin_dict[k][0] in cols_to_go:
                if origin_dict[k][0] not in index_mapper.keys():
                    index_mapper[origin_dict[k][0]] = [k]
                else:
                    index_mapper[origin_dict[k][0]].append(k)

        mean_to_go = [tuple(index_mapper[i]) for i in index_mapper.keys()]

        has_count_for_mean = False

        if len(groupby_cols) == 0:
            raise ValueError()
        elif len(groupby_cols) == 1:
            only_col = groupby_cols[0]
            if only_col in col_ins.keys():
                dict_key_ir = col_ins[only_col]
                if isinstance(dict_key_ir, FlexIR):
                    dict_key_ir = dict_key_ir.sdql_ir
            else:
                dict_key_ir = self.aggr_on.key_access(only_col)
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
            dict_key = list(aggr_dict.items())[0][0]
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

                    if 'sum_for_mean' in k:
                        if v_name in cols_to_go:
                            continue

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
                    if 'count_for_mean' in k:
                        continue
                    else:
                        val_tuples.append((k, v))
                        global_count_name = k
                        has_count_for_mean = True
                else:
                    raise NotImplementedError

            aggr_body = DicConsExpr([(dict_key_ir, RecConsExpr(val_tuples))])

        prev_agg = []

        if cond:
            need_mapper = False
            cond_mapper = {}

            prev_df = []
            for c in self.retriever.findall_cols_in_cond(cond, True):
                if c.field not in self.aggr_on.columns:
                    if c.col_of.name not in prev_df:
                        if c.col_of.retriever.was_aggr:
                            need_mapper = True
                            prev_df.append(c.col_of.name)
                            prev_agg.append(c.col_of.get_aggr(as_part=True))

                    cond_mapper[c.field] = c.col_of.var_aggr

            if need_mapper:
                aggr_body = IfExpr(condExpr=cond.replace(rec=None, inplace=False, mapper=cond_mapper),
                                   thenBodyExpr=aggr_body,
                                   elseBodyExpr=ConstantExpr(None))
            else:
                aggr_body = IfExpr(condExpr=cond.sdql_ir,
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
                    if k in cols_mean:
                        raise NotImplementedError
                    else:
                        format_key_tuples.append((k,
                                                  RecAccessExpr(PairAccessExpr(self.var_x_aggr,
                                                                               1),
                                                                k)))

                format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                          ConstantExpr(True))])
            else:
                # aggr = {record : record}
                format_key_tuples = [(i, RecAccessExpr(PairAccessExpr(self.var_x_aggr, 0), i)) for i in groupby_cols]

                if cols_mean:
                    mean_buffer = {}
                    for k in aggr_dict.keys():
                        if 'for_mean' in k:
                            if 'sum_for_mean' in k:
                                striped_key = k.replace('_sum_for_mean', '')
                                for i in mean_to_go:
                                    if striped_key in i:
                                        mean_buffer[k] = RecAccessExpr(PairAccessExpr(self.var_x_aggr, 1),
                                                                       [j for j in i if j != striped_key][0])
                                        break
                                else:
                                    mean_buffer[k] = RecAccessExpr(
                                        PairAccessExpr(self.var_x_aggr,
                                                       1), k)
                            else:
                                count_expr = RecAccessExpr(PairAccessExpr(self.var_x_aggr,
                                                                          1),
                                                           global_count_name)

                                format_key_tuples.append((mean_mapper[k],
                                                          DivExpr(mean_buffer[k.replace('count_for_mean', 'sum_for_mean')],
                                                                  count_expr)
                                                          ))
                        else:
                            format_key_tuples.append((k,
                                                      RecAccessExpr(PairAccessExpr(self.var_x_aggr,
                                                                                   1),
                                                                    k)))

                    format_op = DicConsExpr([(RecConsExpr(format_key_tuples),
                                              ConstantExpr(True))])
                else:
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

            if prev_agg:
                return SDQLInspector.concat_bindings(prev_agg + [isin_let_expr, aggr_let_expr, form_let_expr])

            return SDQLInspector.concat_bindings([isin_let_expr, aggr_let_expr, form_let_expr])
        else:
            return SDQLInspector.concat_bindings([aggr_let_expr, form_let_expr])

    def get_groupby_aggr_expr(self, next_op, as_part=False) -> LetExpr:
        if as_part:
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
                aggr_body = IfExpr(condExpr=cond.sdql_ir,
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
