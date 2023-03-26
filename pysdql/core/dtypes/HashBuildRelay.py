from pysdql.core.dtypes import MergeExpr
from pysdql.core.util.df_retriever import Retriever


class BuildEnd():
    def __init__(self, build_on):
        self.build_on = build_on
        self.base_join = self.retriever.find_merge('as_part')

    @property
    def retriever(self) -> Retriever:
        return self.build_on.retriever

    @property
    def build_var(self):
        return self.build_on.var_build

    @property
    def probe_on(self):
        return self.base_join.right

    @property
    def build_key(self):
        return self.base_join.left_on

    @property
    def probe_key(self):
        return self.base_join.right_on

    @property
    def build_key_ir(self):
        if isinstance(self.build_key, str):
            pass

        if isinstance(self.build_key, list):
            pass

        raise NotImplementedError(f'Unexpected build key {self.build_key}')

    @property
    def rename_cols(self) -> dict:
        return self.retriever.findall_col_rename()

    @property
    def insert_cols(self) -> dict:
        return self.retriever.findall_col_insert()

    @property
    def project_cols(self) -> list:
        col_proj_op = self.retriever.find_col_proj()

        if col_proj_op:
            return col_proj_op.proj_cols
        else:
            return []

    @property
    def unique_cols(self) -> list:
        return []

    @property
    def modify_cols(self) -> list:
        return []

    def infer_result(self):
        last_iter_op = self.retriever.find_last_iter(body_only=True)

        if isinstance(last_iter_op, MergeExpr):
            pass

        return

    @property
    def sdql_expr(self):
        return

    @property
    def info(self) -> dict:
        return {
            'probe end': self.probe_on.current_name,
            'build key': self.build_key,
            'probe key': self.probe_key,

            'rename columns': self.rename_cols,
            'insert columns': self.insert_cols,
            'project columns': self.project_cols,

            'unique columns': self.unique_cols,
            'modify columns': self.modify_cols,

            'result struct': self.infer_result(),
        }

    def __repr__(self):
        res = f"{self.build_on.current_name} ->\n"

        info_collect = self.info

        for k in info_collect.keys():
            res += f'   | {k}: {info_collect[k]}\n'

        return res