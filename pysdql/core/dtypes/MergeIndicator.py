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