class HavExpr:
    def __init__(self, iter_rec, unit1, op, unit2):
        self.iter_rec = iter_rec
        self.unit1 = unit1
        self.op = op
        self.unit2 = unit2

    def sum(self):
        return self

    @property
    def expr(self):
        return f'{self.unit1} {self.op} {self.unit2}'

    def __repr__(self):
        return self.expr

    def __getitem__(self, item):
        return f'{item}'

    def __gt__(self, other):
        """
        Greater Than ">"
        :param other:
        :return:
        """
        print(f'{self} > {other}')
