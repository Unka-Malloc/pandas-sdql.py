class OpExpr:
    def __init__(self, op_str, op_obj):
        self.op_str = op_str
        self.op_obj = op_obj

        self.data = (self.op_str, self.op_obj)

    @property
    def op_ref_dict(self):
        return {'data': 0}

    @property
    def expr(self):
        return f'{self.op_obj}'

    def __repr__(self):
        return self.expr
