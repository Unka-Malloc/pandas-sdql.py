from pysdql.core.dtypes.JoinPartitionFrame import JoinPartitionFrame
from pysdql.core.dtypes.JoinProbeFrame import JoinProbeFrame

from pysdql.core.dtypes.sdql_ir import (
    SumExpr, IfExpr, VarExpr, ConstantExpr, LetExpr, DicConsExpr, RecConsExpr, SumBuilder, ConcatExpr, MulExpr,
    DicLookupExpr, CompareExpr, CompareSymbol, EmptyDicConsExpr, RecAccessExpr,
)


class JointFrame:
    def __init__(self, partition: JoinPartitionFrame, probe: JoinProbeFrame, joint, col_ins=None, col_proj=None,
                 groupby_cols=None,
                 aggr_dict=None, ):
        self.__partition_frame = partition
        self.__probe_frame = probe
        self.__op = None
        self.__next_op = None
        self.__joint = joint
        self.__col_ins = col_ins if col_ins else {}
        self.__col_proj = col_proj if col_proj else []
        self.groupby_cols = groupby_cols if groupby_cols else []
        self.aggr_dict = aggr_dict if aggr_dict else {}

    @property
    def col_proj(self):
        return self.__col_proj

    @property
    def col_ins(self):
        return self.__col_ins

    def add_col_proj(self, val):
        self.__col_proj = val

    @property
    def partition_frame(self):
        return self.__partition_frame

    @property
    def probe_frame(self):
        return self.__probe_frame

    def add_op(self, val):
        self.__op = val

    def add_next(self, val):
        self.__next_op = val

    @property
    def var_part(self):
        return self.__partition_frame.var_part

    @property
    def var_probe(self):
        return self.__probe_frame.var_probe

    def add_col_proj(self, val):
        self.__probe_frame.add_col_proj(val)

    @property
    def let_expr(self):
        # str: right_on
        probe_key = self.probe_frame.get_probe_key()
        probe_cond = self.probe_frame.get_probe_cond()
        # var:
        part_var = self.partition_frame.get_part_var()
        # var: joint dict
        probe_var = self.probe_frame.get_probe_var()
        # df: probe side
        probe_on = self.probe_frame.get_probe_on()

        probe_key_ir = probe_on.key_access(probe_key)

        col_proj_ir = RecConsExpr(self.col_proj) if self.col_proj else RecConsExpr([(probe_key,
                                                                                     probe_on.key_access(probe_key))])

        if self.groupby_cols and self.aggr_dict:
            # aggr_key_ir
            key_rec_list = []
            for i in self.groupby_cols:
                if i == probe_key:
                    key_rec_list.append((i, probe_on.key_access(i)))
                if i in self.partition_frame.cols_out:
                    key_rec_list.append((i,
                                         RecAccessExpr(recExpr=DicLookupExpr(dicExpr=part_var,
                                                                             keyExpr=probe_on.key_access(i)),
                                                       fieldName=i)))
            aggr_key_ir = RecConsExpr(key_rec_list)

            # aggr_val_ir
            val_rec_list = []
            if self.col_ins:
                for k in self.aggr_dict.keys():
                    v = self.aggr_dict[k]
                    if v.name in self.col_ins.keys():
                        col_expr = self.col_ins[v.name].sdql_ir
                    else:
                        col_expr = v
                    val_rec_list.append((k, col_expr))
            else:
                for k in self.aggr_dict.keys():
                    val_rec_list.append((k, self.aggr_dict[k]))
            aggr_val_ir = RecConsExpr(val_rec_list)

            if probe_cond:
                joint_groupby_aggr_op = IfExpr(condExpr=probe_cond,
                                               thenBodyExpr=IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                                        leftExpr=DicLookupExpr(
                                                                                            dicExpr=part_var,
                                                                                            keyExpr=probe_key_ir),
                                                                                        rightExpr=ConstantExpr(None)),
                                                                   thenBodyExpr=DicConsExpr([(
                                                                       aggr_key_ir,
                                                                       aggr_val_ir
                                                                   )]),
                                                                   elseBodyExpr=EmptyDicConsExpr()),
                                               elseBodyExpr=EmptyDicConsExpr())
            else:
                joint_groupby_aggr_op = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                                    leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                                           keyExpr=probe_key_ir),
                                                                    rightExpr=ConstantExpr(None)),
                                               thenBodyExpr=DicConsExpr([(aggr_key_ir,
                                                                          aggr_val_ir
                                                                          )]),
                                               elseBodyExpr=EmptyDicConsExpr())

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_groupby_aggr_op,
                               isAssignmentSum=False)

            result = VarExpr('result')

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=LetExpr(varExpr=result,
                                              valExpr=SumBuilder(
                                                  lambda p: DicConsExpr([(ConcatExpr(p[0], p[1]), ConstantExpr(True))]),
                                                  probe_var,
                                                  True),
                                              bodyExpr=LetExpr(VarExpr('out'), result, ConstantExpr(True))))

            return output
        else:
            inner_expr = IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                     leftExpr=DicLookupExpr(dicExpr=part_var,
                                                                            keyExpr=probe_key_ir),
                                                     rightExpr=ConstantExpr(None)),
                                thenBodyExpr=DicConsExpr([(
                                    probe_key_ir,
                                    col_proj_ir
                                )]),
                                elseBodyExpr=EmptyDicConsExpr())
            if probe_cond:
                joint_op = IfExpr(condExpr=probe_cond,
                                  thenBodyExpr= inner_expr,
                                  elseBodyExpr=EmptyDicConsExpr())
            else:
                joint_op = inner_expr

            sum_expr = SumExpr(varExpr=probe_on.iter_el.sdql_ir,
                               dictExpr=probe_on.var_expr,
                               bodyExpr=joint_op,
                               isAssignmentSum=False)

            result = VarExpr('result')

            output = LetExpr(varExpr=probe_var,
                             valExpr=sum_expr,
                             bodyExpr=ConstantExpr(None))

            return output

    @property
    def sdql_ir(self):
        # self.__probe_frame.add_next(self.__next_op)
        # self.__partition_frame.add_probe()
        print(f'>> let_expr ({self.__joint.name})')
        print(self.let_expr)
        return self.partition_frame.sdql_ir

    def __repr__(self):
        return f'''
        >> Joint Frame ({self.__joint.name}) <<
            ColProj: {self.__col_proj}
            GroupBy: {self.groupby_cols}
            AggrDict: {self.aggr_dict}
        
        >> Partition Frame ({self.__joint.name}):
                {self.__partition_frame}
        >> Probe Frame ({self.__joint.name}): 
            {self.__probe_frame}
        '''
