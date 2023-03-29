class ColProjRename:
    def __init__(self, base_merge, from_left, to_left, from_right, to_right, is_left=False, is_right=False, is_joint=False):
        self.is_left = is_left
        self.is_right = is_right
        self.is_joint = is_joint
        self.base_merge = base_merge
        self.from_left = from_left
        self.to_left = to_left
        self.from_right = from_right
        self.to_right = to_right


    @property
    def oid(self):
        return hash((
            self.from_left,
            self.to_left,
            self.from_right,
            self.to_right
        ))

    @property
    def op_name_suffix(self):
        return f'column_rename_projection'