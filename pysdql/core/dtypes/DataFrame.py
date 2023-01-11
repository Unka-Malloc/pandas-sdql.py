import ast
import base64
import inspect
import re
import string

from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.ColProjExpr import ColProjExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.DataFrameGroupBy import DataFrameGroupBy
from pysdql.core.dtypes.GroupByAgg import GroupByAgg
from pysdql.core.dtypes.IterEl import IterEl
from pysdql.core.dtypes.IterStmt import IterStmt
# from pysdql.core.dtypes.ConcatExpr import ConcatExpr
from pysdql.core.dtypes.CaseExpr import CaseExpr
from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.ColExpr import ColExpr
from pysdql.core.dtypes.DataFrameColumns import DataFrameColumns
from pysdql.core.dtypes.DataFrameStruct import DataFrameStruct
from pysdql.core.dtypes.ExternalExpr import ExternalExpr
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.JointFrame import JointFrame
from pysdql.core.dtypes.MergeExpr import MergeExpr
from pysdql.core.dtypes.NonNullExpr import NonNullExpr
from pysdql.core.dtypes.OptStmt import OptStmt
from pysdql.core.dtypes.Optimizer import Optimizer
from pysdql.core.dtypes.SumStmt import SumStmt
from pysdql.core.dtypes.SumOpt import SumOpt
from pysdql.core.dtypes.VirColEl import VirColEl
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.OpSeq import OpSeq
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.SemiRing import SemiRing
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.VarBindExpr import VarBindExpr
from pysdql.core.dtypes.VarBindSeq import VarBindSeq

from pysdql.core.dtypes.sdql_ir import *

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
    MathSymbol, OptGoal, SumIterType, AggrType, OperationReturnType,
)


class DataFrame(SemiRing):
    def __init__(self, data=None,
                 index=None,
                 columns=None,
                 dtype=None,
                 name=None,
                 operations=None,
                 is_joint=False,
                 context_variable=None,
                 context_constant=None,
                 context_unopt=None):
        self.__default_name = 'R'
        self.__data = data
        self.__index = index
        self.__columns = columns
        self.__dtype = dtype
        self.__name = name
        self.__var_name = varname()
        self.__operations = operations if operations else OpSeq()

        self.__structure = DataFrameStruct('1DT')

        self.__columns_in = columns if columns else []
        self.__columns_out = columns if columns else []
        self.__columns_used = []

        self.__iter_el = IterEl(f'x_{self.get_name()}')
        self.__var_expr = self.init_var_expr()

        self.context_constant = {}

        self.__is_merged = is_joint

        self.context_variable = context_variable if context_variable else {}
        self.context_constant = context_constant if context_constant else {}

        self.init_context_variable()

        if is_joint:
            vname_part = f'{self.get_name()}'
            self.__var_merge_part = VarExpr(vname_part)
            self.add_context_variable(vname_part,
                                      self.__var_merge_part)
        else:
            vname_part = f'{self.get_name()}_part'
            self.__var_merge_part = VarExpr(vname_part)
            self.add_context_variable(vname_part,
                                      self.__var_merge_part)

        self.unopt_count = 0
        self.unopt_vars = {}
        self.unopt_consts = {}
        self.unopt_list = context_unopt if context_unopt else []

    @property
    def is_joint(self):
        return self.__is_merged

    @property
    def is_merged(self):
        return self.__is_merged

    def add_const(self, const):
        if type(const) == str:
            if const not in self.context_constant.keys():
                tmp_vname = (''.join(re.split(r'[^A-Za-z0-9]', const))).lower()
                tmp_var = VarExpr(tmp_vname)
                self.context_constant[const] = tmp_var
                self.context_variable[tmp_vname] = tmp_var
        else:
            raise ValueError

    def get_const_var(self, const):
        return self.context_constant[const]

    def pre_def_var_const(self):
        pass

    def init_var_expr(self):
        if self.name == 'li':
            return VarExpr("db->li_dataset")
        if self.name == 'cu':
            return VarExpr("db->cu_dataset")
        if self.name == 'ord':
            return VarExpr("db->ord_dataset")
        if self.name == 'pa':
            return VarExpr("db->pa_dataset")
        if self.name == 'su':
            return VarExpr('db->su_dataset')

        return VarExpr(self.name)

    @property
    def var_expr(self):
        return self.__var_expr

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
        if self.name == 'li':
            return ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                    'l_discount',
                    'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                    'l_shipinstruct',
                    'l_shipmode', 'l_comment']
        if self.name == 'na':
            return ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
        if self.name == 'ord':
            return ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                    'o_clerk', 'o_shippriority', 'o_comment']
        if self.name == 'cu':
            return ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                    'c_comment']
        if self.name == 'pa':
            return ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice',
                    'p_comment']
        if self.name == 'su':
            return ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
        if self.name == 'ps':
            return ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']

        if self.__columns:
            return DataFrameColumns(self, self.__columns)
        else:
            if self.__data:
                self.__columns = list(self.__data.keys())
                return DataFrameColumns(self, self.__columns)
            else:
                return DataFrameColumns(self, [])

    @property
    def cols_in(self):
        if self.__columns_in:
            return self.__columns_in
        return self.columns

    @property
    def cols_out(self):
        if self.__columns_out:
            return self.__columns_out
        if self.cols_in:
            return self.cols_in
        return self.columns

    @property
    def cols_used(self):
        self.infer_col_used()
        return [i for i in list(set(self.__columns_used)) if i in self.cols_in]

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

    def get_name(self):
        if self.__name:
            return self.__name
        if self.__var_name:
            return self.__var_name
        return self.__default_name

    def get_var_part(self):
        return self.__var_merge_part

    @property
    def var_part(self):
        return self.__var_merge_part

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
    def iter_el(self) -> IterEl:
        return self.__iter_el

    @property
    def iter_key(self):
        return self.iter_el.key

    @property
    def el(self):
        return self.iter_el

    def key_access(self, field):
        self.__columns_used.append(field)
        if self.is_joint:
            if field in self.partition_side.columns:
                return self.partition_side.key_access(field)
            elif field in self.probe_side.columns:
                return self.probe_side.key_access(field)
        return RecAccessExpr(self.iter_el.key, field)

    def val_access(self, field):
        return RecAccessExpr(self.iter_el.value, field)

    def optimize(self) -> str:
        opt = self.get_opt()
        for op_expr in self.operations:
            opt.input(op_expr)
        query = opt.output

        last_list = [self.define_variables(),
                     f'query = {self.define_constants().get_sdql_ir(query)}']

        return '\n'.join(last_list)

    @property
    def sdql_ir(self):
        opt = self.get_opt()
        for op_expr in self.operations:
            opt.input(op_expr)
        return opt.output

    @property
    def expr(self) -> str:
        if self.name:
            return self.name
        return self.data.expr

    def __repr__(self):
        return self.expr

    @property
    def sdql_expr(self):
        return

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

        if type(item) == list:
            self.__columns_out = item
            self.__columns_used += item

            self.operations.push(OpExpr(op_obj=ColProjExpr(self, item),
                                        op_on=self,
                                        op_iter=False))

            return self

        if type(item) == IsInExpr:
            # self.operations.push(OpExpr(op_obj=item,
            #                             op_on=self,
            #                             op_iter=True))

            return self

    def __getattr__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

    def get_col(self, col_name):
        if col_name in self.columns:
            self.__columns_used.append(col_name)
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
            if type(value) in (ColEl, ColExpr, CaseExpr, ExternalExpr, IfExpr):
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
        self.__columns_used += cols
        return DataFrameGroupBy(groupby_from=self,
                                groupby_cols=cols)

    @property
    def name_ops(self) -> str:
        output = self.name
        for op_expr in self.operations:
            output += op_expr.get_op_name_suffix()
        return output

    def merge(self, right, how='inner', left_on=None, right_on=None):
        next_context_var = {}
        for k in self.context_variable.keys():
            next_context_var[k] = self.context_variable[k]
        for k in right.context_variable.keys():
            next_context_var[k] = right.context_variable[k]

        next_context_const = {}
        for k in self.context_constant.keys():
            next_context_const[k] = self.context_constant[k]
        for k in right.context_constant.keys():
            next_context_const[k] = right.context_constant[k]

        next_context_unopt = self.unopt_list + right.unopt_list

        tmp_name = f'{self.name}_{right.name}'

        tmp_df = DataFrame(name=tmp_name,
                           is_joint=True,
                           context_variable=next_context_var,
                           context_constant=next_context_const,
                           context_unopt=next_context_unopt)

        merge_expr = MergeExpr(left=self,
                               right=right,
                               how=how,
                               left_on=left_on,
                               right_on=right_on,
                               joint=tmp_df)

        self.push(OpExpr(op_obj=merge_expr,
                         op_on=self,
                         op_iter=True))

        right.push(OpExpr(op_obj=merge_expr,
                          op_on=right,
                          op_iter=True))

        tmp_df.push(OpExpr(op_obj=merge_expr,
                           op_on=self,
                           op_iter=True))

        return tmp_df

    def merge_partition_stmt(self):
        return self.get_opt(OptGoal.JoinPartition).merge_partition_stmt()

    def merge_probe_stmt(self, let_next=None, sum_type=SumIterType.Update):
        isAssign = False
        if sum_type == SumIterType.Assign:
            isAssign = True
        if sum_type == SumIterType.Update:
            isAssign = False
        return self.get_opt(OptGoal.JoinProbe).merge_probe_stmt(let_next, isAssign)

    def get_opt(self, opt_goal=OptGoal.UnOptimized):
        opt = Optimizer(opt_on=self,
                        opt_goal=opt_goal)
        for op_expr in self.operations:
            opt.input(op_expr)
        return opt

    def agg(self, func):
        if type(func) == dict:
            return self.agg_by_dict(func)

    def agg_by_dict(self, input_aggr_dict):
        output_aggr_dict = {}

        for aggr_key in input_aggr_dict.keys():
            aggr_func = input_aggr_dict[aggr_key]

            if aggr_func == 'sum':
                output_aggr_dict[aggr_key] = aggr_key
            if aggr_func == 'count':
                output_aggr_dict[aggr_key] = 1

        aggr_expr = AggrExpr(aggr_type=AggrType.DICT,
                             aggr_on=self,
                             aggr_op=output_aggr_dict,
                             aggr_else=EmptyDicConsExpr())

        op_expr = OpExpr(op_obj=aggr_expr,
                         op_on=self,
                         op_iter=True,
                         iter_on=self,
                         ret_type=OperationReturnType.DICT)

        self.push(op_expr)

        return self

    def peak(self):
        return self.operations.peak()

    def show_info(self):
        if self.is_joint:
            self.partition_side.show_info()
            self.probe_side.show_info()

        print(f'>> {self.name} Columns(In) <<')
        print(self.cols_in)
        print(f'>> {self.name} Columns(Out) <<')
        print(self.cols_out)
        print(f'>> {self.name} Columns(Used) <<')
        print(self.cols_used)
        if self.context_variable:
            print(f'>> {self.name} Context Variables <<')
            print(self.context_variable)
        if self.context_constant:
            print(f'>> {self.name} Context Constant <<')
            print(self.context_constant)
        print(f'>> {self.name} Operation Sequence <<')
        print(self.operations)

    def show(self):
        self.show_info()
        print(f'>> {self.name} Optimizer Output <<')
        print(self.optimize())
        # print(f'>> {self.name} Recursive Output <<')
        # print(self.unoptimize())
        print('>> Done <<')

    @property
    def partition_frame(self):
        return self.get_opt(OptGoal.JoinPartition).partition_frame

    @property
    def part_frame(self):
        return self.get_opt(OptGoal.JoinPartition).partition_frame

    def get_partition_frame(self):
        return self.get_opt(OptGoal.JoinPartition).partition_frame

    def get_part_frame(self):
        return self.get_opt(OptGoal.JoinPartition).partition_frame

    @property
    def probe_frame(self):
        return self.get_opt(OptGoal.JoinProbe).probe_frame

    def get_probe_frame(self):
        return self.get_opt(OptGoal.JoinProbe).probe_frame

    def get_joint_frame(self):
        return self.get_opt(OptGoal.Joint).joint_frame

    @property
    def partition_side(self):
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                if self.name != op_expr.op.left.name and self.name != op_expr.op.right.name:
                    return op_expr.op.left

    def get_partition_side(self):
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                if self.name != op_expr.op.left.name and self.name != op_expr.op.right.name:
                    return op_expr.op.left

    @property
    def probe_side(self):
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                if self.name != op_expr.op.left.name and self.name != op_expr.op.right.name:
                    return op_expr.op.right

    def get_probe_side(self):
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                if self.name != op_expr.op.left.name and self.name != op_expr.op.right.name:
                    return op_expr.op.right

    def find_agg(self):
        for op_expr in self.operations:
            if op_expr.op_type == AggrExpr:
                return op_expr
        return None

    def find_groupby_agg(self):
        for op_expr in self.operations:
            if op_expr.op_type == GroupByAgg:
                return op_expr
        return None

    def find_this_merge(self):
        if self.is_joint:
            for op_expr in self.operations:
                if op_expr.op_type == MergeExpr:
                    if self.name == op_expr.op.joint.name:
                        return op_expr
        return None

    def find_next_merge(self):
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                if self.name == op_expr.op.left.name or self.name == op_expr.op.right.name:
                    return op_expr
        return None

    def find_cond(self):
        tmp_list = []
        for op_expr in self.operations:
            if op_expr.op_type == CondExpr:
                tmp_list.append(op_expr)
        if tmp_list:
            return tmp_list
        return None

    def find_col_ins(self):
        tmp_list = []
        for op_expr in self.operations:
            if op_expr.op_type == VirColEl:
                tmp_list.append(op_expr)
        if tmp_list:
            return tmp_list
        return None

    def find_col_proj(self):
        tmp_list = []
        for op_expr in self.operations:
            if op_expr.op_type == ColProjExpr:
                tmp_list.append(op_expr)
        if tmp_list:
            return tmp_list
        return None

    def infer_col_used(self):
        if self.find_next_merge():
            op_expr = self.find_next_merge()
            left = op_expr.op.left
            right = op_expr.op.right

            if self.name == left.name:
                self.__columns_used.append(op_expr.op.left_on)
            if self.name == right.name:
                self.__columns_used.append(op_expr.op.right_on)

            joint_cols_used = op_expr.op.joint.cols_used

            if joint_cols_used:
                self.__columns_used += joint_cols_used

    def find_cols_as_probe_key(self):
        cols_list = []
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                cols_list.append(op_expr.op.right_on)
                if self.name != op_expr.op.joint.name:
                    cols_list += op_expr.op.joint.find_cols_as_probe_key()
        return list(set(cols_list))

    def find_cols_as_part_key(self):
        cols_list = []
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                cols_list.append(op_expr.op.left_on)
                if self.name != op_expr.op.joint.name:
                    cols_list += op_expr.op.joint.find_cols_as_part_key()
        return list(set(cols_list))

    def find_cols_as_key_tuple(self):
        cols_list = []
        for op_expr in self.operations:
            if op_expr.op_type == MergeExpr:
                cols_list.append((op_expr.op.left_on, op_expr.op.right_on))
                if self.name != op_expr.op.joint.name:
                    cols_list += op_expr.op.joint.find_cols_as_key_tuple()
        return list(set(cols_list))

    def get_name_ops(self):
        output = self.name
        for op_expr in self.operations:
            output += op_expr.get_op_name_suffix()
        return output

    # def find_cols_used(self):
    #     cols_list = []
    #     if self.find_next_merge():
    #         op_expr = self.find_next_merge()
    #         left = op_expr.op.left
    #         right = op_expr.op.right
    #
    #         if self.name == left.name:
    #             cols_list.append(op_expr.op.left_on)
    #         if self.name == right.name:
    #             cols_list.append(op_expr.op.right_on)
    #
    #         joint_cols_used = op_expr.op.joint.find_cols_used()
    #
    #         if joint_cols_used:
    #             cols_list += joint_cols_used
    #
    #     return cols_list

    def init_context_variable(self):
        self.context_variable[self.name] = self.var_expr
        self.context_variable[self.iter_el.name] = self.iter_el.el

    def add_context_variable(self, vname, vobj):
        self.context_variable[vname] = vobj

    def init_context_constant(self):
        pass

    def define_variables(self):
        result = ''
        for vname in self.context_variable.keys():
            if vname == 'li':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            elif vname == 'cu':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            elif vname == 'ord':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            elif vname == 'na':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            elif vname == 'pa':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            elif vname == 'su':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            elif vname == 'ps':
                result += f"{vname} = VarExpr('db->{vname}_dataset')\n"
            else:
                result += f"{vname} = VarExpr('{vname}')\n"
        return result

    def define_constants(self):
        result_seq = VarBindSeq()
        for const in self.context_constant.keys():
            result_seq.push(VarBindExpr(var_expr=self.get_const_var(const),
                                        var_value=ConstantExpr(const)))
        return result_seq

    def get_groupby_agg(self, next_op=None):
        return self.get_opt(OptGoal.GroupByAggregation).get_groupby_aggr_stmt(next_op)

    def get_having(self, next_op=None):
        return self.get_opt(OptGoal.GroupByAggregation).isin_op.get_ref_ir(next_op)

    def reset_index(self):
        return self

    def unoptimize(self):
        for op_expr in self.operations:
            tmp_vname = f'v{self.unopt_count}'
            tmp_var = VarExpr(tmp_vname)

            if self.unopt_count == 0:
                last_var = self.var_expr
                iter_last_var = self.iter_el.el
            else:
                last_vname = f'v{self.unopt_count - 1}'
                last_var = VarExpr(last_vname)
                iter_last_var = VarExpr(f'x_{last_vname}')
            iter_last_key = PairAccessExpr(iter_last_var, 0)
            iter_last_val = PairAccessExpr(iter_last_var, 1)
            if op_expr.op_type == CondExpr:
                sum_expr = SumExpr(iter_last_var,
                                   last_var,
                                   IfExpr(op_expr.op,
                                          DicConsExpr([(iter_last_key, iter_last_val)]),
                                          EmptyDicConsExpr()))
                self.unopt_list.append(LetExpr(tmp_var,
                                               sum_expr,
                                               ConstantExpr(True)))
            if op_expr.op_type == VirColEl:
                sum_expr = SumExpr(iter_last_var,
                                   last_var,
                                   DicConsExpr([(ConcatExpr(iter_last_key,
                                                            RecConsExpr([(op_expr.op.col_var,
                                                                          op_expr.op.replace(iter_last_key))])
                                                            ),
                                                 ConstantExpr(True))]))
                self.unopt_list.append(LetExpr(tmp_var,
                                               sum_expr,
                                               ConstantExpr(True)))
            if op_expr.op_type == AggrExpr:
                dic_cons_list = []
                for k in op_expr.op.aggr_op.keys():
                    v = op_expr.op.aggr_op[k]
                    dic_cons_list.append((k, RecAccessExpr(iter_last_key, v)))
                sum_expr = SumExpr(iter_last_var,
                                   last_var,
                                   DicConsExpr(dic_cons_list))
                self.unopt_list.append(LetExpr(tmp_var,
                                               sum_expr,
                                               ConstantExpr(True)))
            if op_expr.op_type == GroupByAgg:
                pass

            self.unopt_count += 1

        result = ''
        for i in range(self.unopt_count):
            result += f"v{i} = VarExpr('v{i}')\n"
            result += f"x_v{i} = VarExpr('x_v{i}')\n"
        result += f"out = VarExpr('out')\n"

        last_seq = VarBindSeq()
        for i in self.unopt_list:
            last_seq.push(VarBindExpr(i.varExpr, i.valExpr))

        result += f'{last_seq.get_sdql_ir(ConstantExpr(True))}'

        return result

    def apply(self, func, axis):
        """

        :param func:
        :param axis: 0=columns, 1 = rows
        :return:
        """
        code = str(inspect.getsource(func)).strip()
        # tree = ast.parse(code)
        # nodes = ast.walk(tree)
        # print(ast.dump(tree, indent=4))

        lamb_arg = re.search(r'lambda.*:', code).group()
        lamb_arg = lamb_arg.replace('lambda', '').replace(':', '').strip()

        lamb_op = re.search(r':.*if', code).group()
        lamb_op = lamb_op.replace(':', '').replace('if', '').strip()

        lamb_cond = re.search(r'if.*else', code).group()
        lamb_cond = lamb_cond.replace('if', '').replace('else', '').strip()

        lamb_else = re.search(r'else.*,', code).group()
        lamb_else = float(lamb_else.replace('else', '').replace(',', '').strip())

        op = eval(lamb_op.replace(f'{lamb_arg}[', 'self['))
        cond = eval(lamb_cond.replace(f'{lamb_arg}[', 'self['))

        if self.is_joint:
            self.get_partition_side().push(OpExpr(op_obj=cond,
                                                  op_on=self,
                                                  op_iter=False))

            return IfExpr(condExpr=NonNullExpr(self.get_partition_side().get_var_part(), ConstantExpr(None)).sdql_ir,
                          thenBodyExpr=op,
                          elseBodyExpr=ConstantExpr(lamb_else))
