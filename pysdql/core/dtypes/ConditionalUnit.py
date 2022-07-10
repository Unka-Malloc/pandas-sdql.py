class CondUnit:
    def __init__(self, unit1, operator: str, unit2):
        self.unit1 = unit1
        self.op = operator
        self.unit2 = unit2

    def new_expr(self, new_str) -> str:
        from pysdql.core.dtypes.ColumnUnit import ColUnit
        if type(self.unit1) == ColUnit or type(self.unit1) == CondUnit:
            u1_str = self.unit1.new_expr(new_str)
        else:
            u1_str = str(self.unit1)
        if type(self.unit2) == ColUnit or type(self.unit2) == CondUnit:
            u2_str = self.unit2.new_expr(new_str)
        else:
            u2_str = str(self.unit2)
        return f'{u1_str} {self.op} {u2_str}'

    def new_expr_with_if(self, new_str) -> str:
        return f'if ({self.new_expr(new_str)})'

    def get_1st(self):
        return str(self.unit1)

    def get_2nd(self):
        return str(self.unit2)

    @property
    def expr_without_if(self) -> str:
        """
        Conditional Expression without 'if'
        :return:
        """
        if self.op in ['==', '>', '>=', '<', '<=', '!=']:
            return f'{self.unit1} {self.op} {self.unit2}'
        if self.op == '&&':
            return f'{self.unit1} {self.op} {self.unit2}'
        if self.op == '||':
            return f'({self.unit1}) {self.op} ({self.unit2})'
        return ''

    @property
    def expr_with_if(self):
        """
        Conditional Expression with 'if'
        :return:
        """
        return f'if ({self.expr_without_if})'

    @property
    def expr(self):
        return self.expr_without_if

    def __repr__(self):
        return self.expr

    def __and__(self, other):
        return CondUnit(unit1=self,
                        operator='&&',
                        unit2=other)

    def __or__(self, other):
        return CondUnit(unit1=self,
                        operator='||',
                        unit2=other)
