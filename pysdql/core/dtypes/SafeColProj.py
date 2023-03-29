from typing import List

from pysdql.core.dtypes.sdql_ir import RecConsExpr

class SafeColProjExpr:
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
