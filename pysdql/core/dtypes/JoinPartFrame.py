from pysdql.core.dtypes.sdql_ir import (
    IfExpr,
    DicConsExpr,
    RecConsExpr,
    EmptyDicConsExpr,
    SumExpr,
    LetExpr,
    ConstantExpr
)
from pysdql.core.util.df_retriever import Retriever


class JoinPartFrame:
    def __init__(self, iter_on, col_proj):
        self.__group_key = None
        self.__iter_cond = None
        self.__col_proj = col_proj if col_proj else []
        self.__iter_on = iter_on
        self.__var_partition = iter_on.get_var_part()
        self.__next_probe = None

    def get_part_col_proj(self):
        if self.__col_proj:
            return self.__col_proj

    @property
    def retriever(self) -> Retriever:
        return self.get_part_on().get_retriever()

    @property
    def next_probe(self):
        return self.__next_probe

    @property
    def part_on_iter_el(self):
        return self.part_on.iter_el.sdql_ir

    @property
    def group_key(self):
        return self.__group_key

    @property
    def part_cond(self):
        return self.__iter_cond

    @property
    def part_key(self):
        return self.__group_key

    @property
    def partition_on(self):
        return self.__iter_on

    @property
    def part_on(self):
        return self.__iter_on

    def get_part_var(self):
        return self.__var_partition

    @property
    def part_var(self):
        return self.__var_partition

    @property
    def cols_out(self):
        return self.__iter_on.cols_out

    def get_part_on(self):
        return self.__iter_on

    def get_partition_on(self):
        return self.__iter_on

    @property
    def col_proj_ir(self):
        col_proj = self.get_part_col_proj()
        if col_proj:
            tmp_col_proj = [col for col in col_proj if self.group_key != col[0]]
            if tmp_col_proj:
                return RecConsExpr(tmp_col_proj)
            else:
                return RecConsExpr(col_proj)
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
    def is_joint(self):
        return self.partition_on.is_joint

    def get_part_key(self):
        return self.__group_key

    def get_part_expr(self, next_probe_op=None):
        if not next_probe_op:
            if self.next_probe:
                next_probe_op = self.next_probe
            else:
                next_probe_op = ConstantExpr('placeholder_partition_next')

        if isinstance(self.part_key, str):
            part_left_op = DicConsExpr([(
                self.part_on.key_access(self.group_key),
                self.col_proj_ir
            )])

            if self.has_cond:
                part_left_op = IfExpr(condExpr=self.__iter_cond,
                                      thenBodyExpr=part_left_op,
                                      elseBodyExpr=EmptyDicConsExpr())

            part_left_sum = SumExpr(varExpr=self.part_on_iter_el,
                                    dictExpr=self.part_on.var_expr,
                                    bodyExpr=part_left_op,
                                    isAssignmentSum=True)

            part_left_let = LetExpr(varExpr=self.part_var,
                                    valExpr=part_left_sum,
                                    bodyExpr=next_probe_op)

            return part_left_let
        elif isinstance(self.part_key, list):
            if self.retriever.as_bypass_for_next_join:
                part_left_op = DicConsExpr([(RecConsExpr([(k, self.part_on.key_access(k)) for k in self.group_key]),
                                             ConstantExpr(True))])

                if self.has_cond:
                    part_left_op = IfExpr(condExpr=self.part_cond,
                                          thenBodyExpr=part_left_op,
                                          elseBodyExpr=EmptyDicConsExpr())

                part_left_sum = SumExpr(varExpr=self.part_on_iter_el,
                                        dictExpr=self.__iter_on.var_expr,
                                        bodyExpr=part_left_op,
                                        isAssignmentSum=True)

                part_left_let = LetExpr(varExpr=self.part_var,
                                        valExpr=part_left_sum,
                                        bodyExpr=next_probe_op)

                return part_left_let
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

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

    def __repr__(self):
        if self.partition_on.is_joint:
            joint_frame = self.partition_on.get_joint_frame()
            return repr(joint_frame)

        return str(
            {
                'patition': 'frame',
                'part_key': self.__group_key,
                'cond': self.__iter_cond,
                'cols': self.__col_proj,
                'var': self.__var_partition
            }
        )
