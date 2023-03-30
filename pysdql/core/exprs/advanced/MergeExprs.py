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

    @property
    def oid(self):
        return hash((
            self.left.oid,
            self.right.oid,
            str(self.left_on),
            str(self.right_on),
            self.joint.oid,
        ))


class MergeIndicator:
    def __init__(self):
        self.left_only = False
        self.right_only = False

    def __eq__(self, other):
        if other == "left_only":
            self.left_only = True
            return self

        if other == "right_only":
            self.right_only = True
            return self

        else:
            raise NotImplementedError

    @property
    def op_name_suffix(self):
        return f'_merge_indicator'

    def __repr__(self):
        if self.left_only:
            return f'MergeIndicator(left_only)'

        if self.right_only:
            return f'MergeIndicator(right_only)'

        return f'MergeIndicator( )'