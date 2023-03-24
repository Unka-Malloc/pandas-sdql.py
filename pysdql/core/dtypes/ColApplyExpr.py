from pysdql.core.dtypes.SDQLInspector import SDQLInspector
from pysdql.core.dtypes.sdql_ir import IfExpr


class ColApplyExpr:
    def __init__(self, apply_op, apply_cond=None, apply_else=None, unopt_cond=None, more_cond=None, original_column=None):
        self.apply_op = apply_op

        self.apply_cond = apply_cond
        self.apply_else = apply_else

        self.unopt_cond = unopt_cond
        self.more_cond = more_cond if more_cond else []

        self.original_column = original_column

    def replace(self, rec, inplace=False, mapper=None):
        if rec:
            if inplace:
                raise NotImplementedError
            else:
                self.apply_op = SDQLInspector.replace_access(self.apply_op, rec)
        else:
            if mapper:
                new_mapper = {}

                for k in mapper.keys():
                    if isinstance(k, (tuple, )):
                        for el in k:
                            new_mapper[el] = mapper[k]
                    else:
                        new_mapper[k] = mapper[k]

                self.apply_cond = SDQLInspector.replace_field(sdql_obj=self.apply_cond,
                                                              inplace=inplace,
                                                              mapper=new_mapper)

                self.apply_op = SDQLInspector.replace_field(sdql_obj=self.apply_op,
                                                              inplace=inplace,
                                                              mapper=new_mapper)

        return self.sdql_ir

    @property
    def original_unopt_sdql_ir(self):
        if self.apply_cond:
            result = IfExpr(self.unopt_cond,
                            self.original_column.sdql_ir,
                            self.apply_else)
        else:
            result = self.original_column.sdql_ir
        return result

    @property
    def unopt_sdql_ir(self):
        if self.apply_cond:
            result = IfExpr(self.unopt_cond,
                          self.apply_op,
                          self.apply_else)
        else:
            result = self.apply_op
        return result

    @property
    def sdql_ir(self):
        if self.apply_cond:
            result = IfExpr(self.apply_cond,
                          self.apply_op,
                          self.apply_else)
        else:
            result = self.apply_op

        if self.more_cond:
            for cond in self.more_cond:
                result = IfExpr(cond,
                                result,
                                self.apply_else)

        return result

    def __repr__(self):
        return str(self.sdql_ir)