class SumStmt:
    def __init__(self, sum_on, sum_func, sum_if, sum_else, sum_update):
        self.sum_on = sum_on
        self.sum_func = sum_func
        self.sum_if = sum_if
        self.sum_else = sum_else
        self.sum_update = sum_update

    @property
    def expr(self) -> str:
        return f'{self.sum_on.name}.sum(lambda p: {self.sum_func} if {self.sum_if} else {self.sum_else}, {self.sum_update})'

    def __repr__(self):
        return self.expr

    def add_cond(self, cond_if):
        if self.sum_if:
            new_if = f'{self.sum_if} and {cond_if}'
        else:
            new_if = f'{cond_if}'
        return SumStmt(sum_on=self.sum_on,
                       sum_func=self.sum_func,
                       sum_if=new_if,
                       sum_else=self.sum_else,
                       sum_update=self.sum_update)

    @property
    def op_name_suffix(self):
        return f'_sum'