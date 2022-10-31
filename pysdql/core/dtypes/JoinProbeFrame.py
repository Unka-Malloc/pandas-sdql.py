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
