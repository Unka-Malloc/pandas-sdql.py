from pysdql.core.dtypes.sdql_ir import IfExpr, DicConsExpr, RecConsExpr, EmptyDicConsExpr, SumExpr, LetExpr


class JoinPartitionFrame:
    def __init__(self, iter_on):
        self.__group_key = None
        self.__iter_cond = None
        self.__col_proj = None
        self.__iter_el = iter_on.iter_el.sdql_ir
        self.__iter_on = iter_on
        self.__var_partition = iter_on.get_var_part()
        self.__next_probe = None

    @property
    def group_key(self):
        return self.__group_key

    @property
    def partition_on(self):
        return self.__iter_on

    def get_part_var(self):
        return self.__var_partition

    @property
    def var_part(self):
        return self.__var_partition

    @property
    def cols_out(self):
        return self.__iter_on.cols_out

    @property
    def col_proj_ir(self):
        if self.__col_proj:
            return RecConsExpr(self.__col_proj)
        else:
            return RecConsExpr([(self.__group_key,
                                 self.__iter_on.key_access(self.__group_key))])

    def add_key(self, val):
        self.__group_key = val

    def add_cond(self, val):
        self.__iter_cond = val

    def add_col_proj(self, val):
        self.__col_proj = val

    def add_probe(self, val):
        self.__next_probe = val

    @property
    def let_expr(self):
        if self.has_cond:
            part_left_op = IfExpr(condExpr=self.__iter_cond,
                                  thenBodyExpr=DicConsExpr([(
                                      self.__iter_on.key_access(self.__group_key),
                                      self.col_proj_ir
                                  )]),
                                  elseBodyExpr=EmptyDicConsExpr())
        else:
            part_left_op = DicConsExpr([(
                self.__iter_on.key_access(self.__group_key),
                self.col_proj_ir
            )])

        part_left_sum = SumExpr(varExpr=self.__iter_el,
                                dictExpr=self.__iter_on.var_expr,
                                bodyExpr=part_left_op,
                                isAssignmentSum=True)

        return LetExpr(varExpr=self.__var_partition,
                       valExpr=part_left_sum,
                       bodyExpr=self.__next_probe)

    @property
    def has_cond(self):
        if self.__iter_cond:
            return True
        else:
            return False

    @property
    def filled(self):
        if self.__group_key:
            return True
        return False

    @property
    def finished(self):
        if self.__group_key and self.__next_probe:
            return True
        return False

    @property
    def sdql_ir(self):
        if self.partition_on.is_joint:
            return self.partition_on.get_joint_frame().sdql_ir
        return repr(self.let_expr)

    def __repr__(self):
        if self.partition_on.is_joint:
            joint_frame = self.partition_on.get_joint_frame()
            return str(joint_frame)

        return str(
            {
                'patition': 'frame',
                'part_key': self.__group_key,
                'cond': self.__iter_cond,
                'cols': self.__col_proj,
                'var': self.__var_partition
            }
        )
