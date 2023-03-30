from typing import List

from pysdql.core.prototype.basic.sdql_ir import RecConsExpr


class ColProj:
    def __init__(self, proj_on, proj_cols: List[str]):
        """
        :param proj_on:
        :param List[str] proj_cols: list of columns to be projected
        """
        self.proj_on = proj_on
        self.__proj_cols = tuple(proj_cols)

    @property
    def proj_cols(self):
        return list(self.__proj_cols)

    @property
    def cols(self):
        return RecConsExpr([(col, self.proj_on.key_access(col)) for col in self.proj_cols])

    @property
    def sdql_ir(self):
        return self.cols

    @property
    def op_name_suffix(self):
        return f'_proj'

    def __repr__(self):
        return str({
            'proj_on': self.proj_on,
            'proj_cols': self.proj_cols
        })

    @property
    def oid(self):
        return hash((
            id(self),
            self.proj_on.oid,
            tuple(self.proj_cols)
        ))


class ColProjUnique:
    def __init__(self, proj_on, proj_cols: List[str]):
        """
        :param proj_on:
        :param List[str] proj_cols: list of columns to be projected
        """
        self.proj_on = proj_on
        self.__proj_cols = tuple(proj_cols)

    @property
    def proj_cols(self):
        return list(self.__proj_cols)

    @property
    def cols(self):
        return RecConsExpr([(col, self.proj_on.key_access(col)) for col in self.proj_cols])

    @property
    def sdql_ir(self):
        return self.cols

    @property
    def op_name_suffix(self):
        return f'_proj'

    def __repr__(self):
        return str({
            'proj_on': self.proj_on,
            'proj_cols': self.proj_cols
        })

    @property
    def oid(self):
        return hash((
            self.proj_on.oid,
            tuple(self.proj_cols)
        ))


class ColProjExtra:
    def __init__(self, add_col):
        """

        :param add_col: list additional columns
        """
        self.add_col = add_col

    @property
    def op_suffix_name(self):
        return f'additional_columns'


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
            tuple(self.from_left),
            tuple(self.to_left),
            tuple(self.from_right),
            tuple(self.to_right),
        ))

    @property
    def op_name_suffix(self):
        return f'column_rename_projection'