class SetExpr:
    def __init__(self, *args):
        self.keys = args

    @property
    def expr(self):
        keys_str = ''
        for i in range(len(self.keys)):
            keys_str += f'{self.keys[i]}'
            if not i == len(self.keys)-1:
                keys_str += ', '

        return f'{{ {keys_str} }}'

    def __repr__(self):
        return self.expr
