from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.IsInExpr import IsInExpr


class JoinProbeFrame:
    def __init__(self, iter_on):
        self.__probe_key = None
        self.__iter_cond = None
        self.__col_proj = None
        self.__iter_el = iter_on.iter_el.sdql_ir
        self.__iter_on = iter_on
        self.__iter_op = None
        self.__partition_frame = None
        self.__next_op = None

    def get_groupby_cols(self):
        for op_expr in self.probe_on.operations:
            if op_expr.op_type == GroupByAgg:
                return op_expr.op.groupby_cols
        else:
            raise ValueError()

    def get_aggr_dict(self):
        for op_expr in self.probe_on.operations:
            if op_expr.op_type == GroupByAgg:
                return op_expr.op.agg_dict
        else:
            raise ValueError()

    def get_cond_after_groupby_agg(self):
        groupby_agg_located = False
        for op_expr in self.probe_on.operations:
            if op_expr.op_type == GroupByAgg:
                groupby_agg_located = True
            if op_expr.op_type == CondExpr:
                if groupby_agg_located:
                    return op_expr.op
        return None

    def get_isin(self):
        for op_expr in self.probe_on.operations:
            if op_expr.op_type == IsInExpr:
                return op_expr.op
        return None

    def get_probe_col_proj(self):
        return self.__col_proj

    def get_probe_key(self):
        return self.__probe_key

    def get_probe_cond(self):
        return self.__iter_cond

    def get_probe_var(self):
        return self.__var_probe

    def get_probe_on(self):
        return self.__iter_on

    def get_probe_on_var(self):
        return self.__iter_on.var_expr

    @property
    def probe_on(self):
        return self.__iter_on

    @property
    def is_joint(self):
        return self.probe_on.is_joint

    @property
    def was_groupby_agg(self):
        for op_expr in self.__iter_on.operations:
            if op_expr.op_type == GroupByAgg:
                return True
        return False

    def add_key(self, val):
        self.__probe_key = val

    def add_partition(self, val):
        self.__partition_frame = val

    def add_cond(self, val):
        self.__iter_cond = val

    def add_col_proj(self, val):
        self.__col_proj = val

    def add_op(self, val):
        self.__iter_op = val

    def add_next(self, val):
        self.__next_op = val

    def __repr__(self):
        if self.probe_on.is_joint:
            joint_frame = self.probe_on.get_joint_frame()
            return str(joint_frame)

        return str(
            {
                'probe': 'frame',
                'probe_key': self.__probe_key,
                'cond': self.__iter_cond,
                'cols': self.__col_proj
            }
        )
