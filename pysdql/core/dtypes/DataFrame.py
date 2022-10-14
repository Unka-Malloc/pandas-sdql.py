import string

from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.DataFrameGroupBy import DataFrameGroupBy
from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.IterEl import IterEl
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.ConcatExpr import ConcatExpr
from pysdql.core.dtypes.CaseExpr import CaseExpr
from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.DataFrameColumns import DataFrameColumns
from pysdql.core.dtypes.DataFrameStruct import DataFrameStruct
from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.OptStmt import OptStmt
from pysdql.core.dtypes.SumExpr import SumExpr
from pysdql.core.dtypes.SumOpt import SumOpt
from pysdql.core.dtypes.VirColEl import VirColEl
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.OpSeq import OpSeq
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.SemiRing import SemiRing

from pysdql.core.dtypes.sdql_ir import (
    Expr,
    MulExpr,
    AddExpr,
    CompareExpr,
)

from pysdql.core.util.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str
)

from pysdql.core.util.data_interpreter import (
    to_scalar
)

from varname import varname

from pysdql.core.dtypes.EnumUtil import (
    LogicSymbol,
    MathSymbol,
)


class DataFrame(SemiRing):
    def __init__(self, data=None, index=None, columns=None, dtype=None, name=None, operations=None):
        self.__default_name = 'R'
        self.__data = data
        self.__index = index
        self.__columns = columns
        self.__dtype = dtype
        self.__name = name
        self.__var_name = varname()
        self.__operations = operations if operations else OpSeq()

        self.__structure = DataFrameStruct('1DT')

    @property
    def data(self):
        if self.__columns:
            columns_names = self.__columns
        else:
            columns_names = list(self.__data.keys())

        data_size = len(self.__data[columns_names[0]])

        rec_dict = {}
        for i in range(data_size):
            tmp_dict = {}
            for k in columns_names:
                tmp_dict[k] = self.__data[k][i]
            rec_dict[RecEl(tmp_dict)] = 1
        return DictEl(rec_dict)

    @property
    def index(self):
        return self.__index

    @property
    def columns(self):
        if self.__columns:
            return DataFrameColumns(self, self.__columns)
        else:
            if self.__data:
                self.__columns = list(self.__data.keys())
                return DataFrameColumns(self, self.__columns)
            else:
                return DataFrameColumns(self, [])

    @property
    def dtype(self):
        if self.__dtype:
            return self.__dtype

        if self.__data:
            tmp_dict = {}
            for k in self.__data.keys():
                first_item = self.__data[k][0]
                if is_int(first_item):
                    tmp_dict[k] = 'int'
                elif is_float(first_item):
                    tmp_dict[k] = 'real'
                elif is_date(first_item):
                    tmp_dict[k] = 'date'
                elif is_str(first_item):
                    tmp_dict[k] = 'string'
                else:
                    raise ValueError(f'Cannot identify type {first_item}')
            return tmp_dict

    @property
    def name(self):
        if self.__name:
            return self.__name
        if self.__var_name:
            return self.__var_name
        return self.__default_name

    # @name.setter
    # def name(self, val):
    #     allow_set_name = False
    #     if self.mutable:
    #         allow_set_name = True
    #     else:
    #         if self.name == self.__default_name:
    #             allow_set_name = True
    #
    #     if allow_set_name:
    #         self.operations.push(OpExpr('', VarExpr(val, self.name)))
    #         self.__name = val

    @property
    def tmp_name_list(self):
        return ['tmp_a', 'tmp_b', 'tmp_c', 'tmp_d', 'tmp_e', 'tmp_f', 'tmp_g',
                'tmp_h', 'tmp_i', 'tmp_j', 'tmp_k', 'tmp_l', 'tmp_m', 'tmp_n',
                'tmp_o', 'tmp_p', 'tmp_q', 'tmp_r', 'tmp_s', 'tmp_t',
                'tmp_u', 'tmp_v', 'tmp_w', 'tmp_x', 'tmp_y', 'tmp_z']

    @staticmethod
    def hard_code_tmp_name():
        name_list = []
        for i in list(string.ascii_lowercase):
            name_list.append(f'tmp_{i}')
        print(name_list)

    def gen_tmp_name(self, noname=None):
        if noname is None:
            noname = [self.name] + self.history_name

        for tmp_name in self.tmp_name_list:
            if tmp_name not in noname:
                return tmp_name
        else:
            for i in range(1024):
                tmp_name = f'tmp_{i}'
                if tmp_name not in noname:
                    return tmp_name
            else:
                raise ValueError('Failed to generate tmp name!')

    @property
    def operations(self):
        return self.__operations

    @property
    def history_name(self):
        return self.operations.names

    def pop(self):
        self.operations.pop()

    def push(self, val):
        self.operations.push(val)

    @property
    def mutable(self):
        if self.__data:
            return False
        return True

    @property
    def iter_el(self):
        return IterEl(str(hash((self.name, ))))

    @property
    def el(self):
        return self.iter_el

    def optimize(self):
        sum_opt = SumOpt(self)
        opt_name = self.name
        for op_expr in self.operations:
            opt_name += op_expr.get_op_name_suffix()

            if op_expr.op_type == CondExpr:
                sum_opt.add_cond(op_expr.op)
            # if op_expr.op_type == SumExpr:
            #     sum_opt.merge(op_expr.op)
            # if op_expr.op_type == VirColEl:
            #     sum_opt.var_cols[op_expr.op.col_var] = op_expr.op.col_expr
            # if op_expr.op_type == GroupByAgg:
            #     sum_opt.groupby_agg(op_expr.op)

        return OptStmt(opt_name=opt_name,
                       opt_sum=sum_opt)

        # output = f'{opt_name} = {sum_opt.expr}'
        # return output

    @property
    def sdql_ir(self):
        return self.optimize().sdql_ir

    @property
    def expr(self) -> str:
        if self.name:
            return self.name
        return self.data.expr

    def __repr__(self):
        return self.expr

    @property
    def sdql_expr(self):
        return self.optimize()

    def __str__(self):
        return self.sdql_expr

    @property
    def structure(self) -> str:
        return self.__structure.type

    @structure.setter
    def structure(self, val: str):
        self.__structure = DataFrameStruct(val)

    def __getitem__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

        if type(item) == CompareExpr:
            return self[CondExpr(unit1=item.leftExpr,
                                 operator=item.compareType,
                                 unit2=item.rightExpr)]
        if type(item) == MulExpr:
            return self[CondExpr(unit1=item.op1Expr,
                                 operator=LogicSymbol.AND,
                                 unit2=item.op2Expr)]
        if type(item) == AddExpr:
            return self[CondExpr(unit1=item.op1Expr,
                                 operator=LogicSymbol.OR,
                                 unit2=item.op2Expr)]

        if type(item) == CondExpr:
            self.operations.push(OpExpr(op_obj=item,
                                        op_on=self,
                                        op_iter=False))
            return self

    def __getattr__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

    def get_col(self, col_name):
        if col_name in self.columns:
            return ColEl(self, col_name)

        # unsafe
        return ColEl(self, col_name)

    def __setitem__(self, key, value):
        if key in self.columns:
            if type(value) in (bool, int, float, str):
                return self.rename_col_scalar(key, value)
            if type(value) in (ColEl, ColExpr, CaseExpr, ExternalExpr):
                return self.rename_col_expr(key, value)
        else:
            if type(value) in (bool, int, float, str):
                return self.insert_col_scalar(key, value)
            if type(value) in (ColEl, ColExpr, CaseExpr, ExternalExpr):
                return self.insert_col_expr(key, value)

    def rename_col_scalar(self, key, value):
        pass

    def rename_col_expr(self, key, value):
        pass

    def insert_col_scalar(self, key, value):
        pass
        # next_name = self.gen_tmp_name()
        # next_df = DataFrame(name=next_name, operations=self.operations)
        #
        # value = to_scalar(value)
        # var = VarExpr(next_name, IterStmt(self.iter_expr,
        #                                   DictEl({ConcatExpr(self.iter_expr.key, RecEl({key: value}))
        #                                           : 1})))
        #
        # next_df.push(OpExpr('', var))

    def insert_col_expr(self, key, value):
        self.operations.push(OpExpr(op_obj=VirColEl(col_var=key,
                                                    col_expr=value),
                                    op_on=self,
                                    op_iter=False))

    def groupby(self, cols):
        return DataFrameGroupBy(groupby_from=self,
                                groupby_cols=cols)
