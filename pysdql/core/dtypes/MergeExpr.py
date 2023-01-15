class MergeExpr:
    def __init__(self, left, right, how, left_on, right_on, joint):
        self.left = left
        self.right = right
        self.how = how
        self.left_on = left_on
        self.right_on = right_on
        self.joint = joint

    @property
    def sdql_ir(self):
        return

    def __repr__(self):
        return str({
            'left': self.left,
            'right': self.right,
            'how': self.how,
            'left_on': self.left_on,
            'right_on': self.right_on,
            'joint': self.joint.name,
        })

    @property
    def op_name_suffix(self):
        return f'_merge'
