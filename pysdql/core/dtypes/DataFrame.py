import inspect
import re
import string

from pysdql.core.dtypes.AggrExpr import AggrExpr
from pysdql.core.dtypes.AggrFrame import AggrFrame
from pysdql.core.dtypes.ColProjExpr import ColProjExpr
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.DataFrameGroupBy import DataFrameGroupBy
from pysdql.core.dtypes.GroupbyAggrExpr import GroupbyAggrExpr
from pysdql.core.dtypes.GroupbyAggrFrame import GroupbyAggrFrame
from pysdql.core.dtypes.IterEl import IterEl
from pysdql.core.dtypes.CaseExpr import CaseExpr
from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.ColOpExpr import ColOpExpr
from pysdql.core.dtypes.DataFrameStruct import DataFrameStruct
from pysdql.core.dtypes.ColExtExpr import ColExtExpr
from pysdql.core.dtypes.MergeExpr import MergeExpr
from pysdql.core.dtypes.OldColOpExpr import OldColOpExpr
from pysdql.core.dtypes.Optimizer import Optimizer
from pysdql.core.dtypes.FlexIR import FlexIR
from pysdql.core.dtypes.TransExpr import TransExpr
from pysdql.core.dtypes.NewColOpExpr import NewColOpExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.OpSeq import OpSeq
from pysdql.core.dtypes.RecEl import RecEl
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.IsInExpr import IsInExpr
from pysdql.core.dtypes.VarBindExpr import VarBindExpr
from pysdql.core.dtypes.VarBindSeq import VarBindSeq
from pysdql.core.util.data_matcher import match_int, match_float
from pysdql.extlib.sdqlir_to_sdqlpy import GenerateSDQLPYCode

from pysdql.core.dtypes.sdql_ir import *

from pysdql.core.util.type_checker import (
    is_int,
    is_float,
    is_date,
    is_str
)

from pysdql.core.util.df_retriever import Retriever

from varname import varname

from pysdql.core.dtypes.EnumUtil import (
    LogicSymbol, OptGoal, SumIterType, AggrType, OpRetType,
)

from pysdql.const import (
    CUSTOMER_COLS,
    LINEITEM_COLS,
    ORDERS_COLS,
    NATION_COLS,
    REGION_COLS,
    PART_COLS,
    SUPPLIER_COLS,
    PARTSUPP_COLS
)

from pysdql.core.interfaces import (
    Retrivable
)


class DataFrame(FlexIR, Retrivable):
    def __init__(self,
                 data=None,
                 index=None,
                 columns=None,
                 dtype=None,
                 name=None,
                 operations=None,
                 is_joint=False,
                 is_original=True,
                 context_variable=None,
                 context_constant=None,
                 context_unopt=None,
                 context_semiopt=None):
        super().__init__()
        self.__default_name = 'R'
        self.__data = data
        self.__index = index
        self.__dtype = dtype
        self.__name = name if name else varname()
        self.__columns = columns if columns else self.preset_cols()
        self.__columns_in = columns if columns else self.preset_cols()
        self.__operations = operations if operations else OpSeq()
        self.__retriever = Retriever(self)

        self.__structure = DataFrameStruct('1DT')

        self.__iter_el = IterEl(f'x_{self.get_name()}')
        self.__var_expr = self.init_var_expr()

        self.__is_merged = is_joint

        self.context_constant = {}

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

        vname_aggr = f'{self.get_name()}_aggr'
        self.__var_aggr = VarExpr(vname_aggr)

        self.unopt_count = 0
        self.unopt_vars = {}
        self.unopt_consts = {}
        self.context_unopt = context_unopt if context_unopt else []

        self.transform = TransExpr(self)

        self.context_semiopt = context_semiopt if context_semiopt else []

        self.original = is_original

    def copy(self):
        new_name = varname()

        return DataFrame(name=new_name,
                         is_joint=self.is_joint,
                         is_original=False,
                         columns=self.columns.copy(),
                         context_variable=self.context_variable,
                         context_constant=self.context_constant,
                         context_unopt=self.context_unopt,
                         context_semiopt=self.context_semiopt)

    @property
    def is_joint(self):
        return self.__is_merged

    @property
    def is_merged(self):
        return self.__is_merged

    def add_const(self, const):
        if type(const) == str:
            if const not in self.context_constant.keys():
                tmp_vname = (''.join(re.split(r'[^A-Za-z]', const))).lower() + ''.join(
                    [i for i in const if i.isdigit()])
                if tmp_vname.isdigit():
                    tmp_vname = f'v{tmp_vname}'
                tmp_var = VarExpr(tmp_vname)
                self.context_constant[const] = tmp_var
                self.context_variable[tmp_vname] = tmp_var
        else:
            raise ValueError

    def get_const_var(self, const):
        return self.context_constant[const]

    def pre_def_var_const(self):
        pass

    @staticmethod
    def map_name(name):
        if name in ['cu', 'customer']:
            return 'customer'
        if name in ['li', 'lineitem']:
            return 'lineitem'
        if name in ['ord', 'orders']:
            return 'orders'
        if name in ['pa', 'part']:
            return 'part'
        if name in ['su', 'supplier']:
            return 'supplier'
        if name in ['ps', 'partsupp']:
            return 'partsupp'
        if name in ['na', 'nation']:
            return 'nation'
        if name in ['re', 'region']:
            return 'region'

    def init_var_expr(self):
        if self.name == 'customer':
            return VarExpr("db->cu_dataset")
        if self.name == 'lineitem':
            return VarExpr("db->li_dataset")
        if self.name == 'orders':
            return VarExpr("db->ord_dataset")
        if self.name == 'nation':
            return VarExpr('db->na_dataset')
        if self.name == 'region':
            return VarExpr('db->re_dataset')
        if self.name == 'part':
            return VarExpr("db->pa_dataset")
        if self.name == 'supplier':
            return VarExpr('db->su_dataset')
        if self.name == 'partsupp':
            return VarExpr('db->ps_dataset')

        return VarExpr(self.name)

    @property
    def var_expr(self):
        return self.__var_expr

    @property
    def data(self):
        if self.columns:
            columns_names = self.columns
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

    '''
    Columns
    Columns In
    Columns Out
    Columns Used
    '''

    def preset_cols(self) -> list:
        if self.__name in ['customer', 'cu']:
            return CUSTOMER_COLS
        if self.__name in ['lineitem', 'li']:
            return LINEITEM_COLS
        if self.__name in ['orders', 'ord']:
            return ORDERS_COLS
        if self.__name in ['nation', 'na']:
            return NATION_COLS
        if self.__name in ['region', 're']:
            return REGION_COLS
        if self.__name in ['part', 'pa']:
            return PART_COLS
        if self.__name in ['supplier', 'su']:
            return SUPPLIER_COLS
        if self.__name in ['partsupp', 'ps']:
            return PARTSUPP_COLS
        return []

    @property
    def columns(self) -> list:
        return self.__columns

    @property
    def cols_in(self) -> list:
        return self.__columns_in

    @property
    def cols_out(self) -> list:
        return self.infer_cols_out()

    def infer_cols_out(self) -> list:
        """
        What could change columns?
        The last operation:
            1. col proj
            2. groupby agg
            3. agg
            4. merge
        :return:
        """
        tmp_cols = []

        rename_cols = {}

        for op_expr in self.operations:
            op_body = op_expr.op

            if isinstance(op_body, NewColOpExpr):
                tmp_cols.append(op_body.col_var)
            elif isinstance(op_body, OldColOpExpr):
                if isinstance(op_body.col_expr, str):
                    rename_cols[op_body.col_var] = op_body.col_expr
                else:
                    raise NotImplementedError
            elif isinstance(op_body, ColProjExpr):
                tmp_cols = op_body.proj_cols
            elif isinstance(op_body, AggrExpr):
                tmp_cols = list(op_body.aggr_op.keys())
            elif isinstance(op_body, GroupbyAggrExpr):
                tmp_cols = op_body.groupby_cols + list(op_body.aggr_dict.keys())
            elif isinstance(op_body, MergeExpr):
                if self.name == op_body.joint.name:
                    tmp_cols = op_body.left.cols_out + op_body.right.cols_out
        else:
            if tmp_cols:
                for k in rename_cols.keys():
                    if k in tmp_cols:
                        tmp_cols[tmp_cols.index(k)] = rename_cols[k]
                    else:
                        raise IndexError(f'{k} not found in {self.name} columns {tmp_cols}')
                return tmp_cols
            else:
                return self.cols_in

    @property
    def cols_used(self):
        return self.retriever.findall_cols_used(as_owner=True)

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
        return self.__default_name

    def get_name(self):
        if self.__name:
            return self.__name
        return self.__default_name

    def get_var_part(self):
        return self.__var_merge_part

    @property
    def var_part(self):
        return self.__var_merge_part

    def get_var_aggr(self):
        return self.__var_aggr

    @property
    def var_aggr(self):
        return self.__var_aggr

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

    def opt_to_sdqlir(self, indent='    ') -> str:
        opt = self.get_opt()
        for op_expr in self.operations:
            opt.input(op_expr)
        query_obj = opt.output

        query_str = GenerateSDQLPYCode(self.define_constants().get_sdql_ir(query_obj), {})

        query_list = query_str.split('\n')

        query_list = query_list[:query_list.index('True')]

        print('>> Optimized Query <<')

        print(f'{"=" * 60}')

        print('\n'.join(query_list))

        print(f'{"=" * 60}')

        query_list = [f'{indent}{i}' for i in query_list]

        return '\n'.join(query_list)

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return False

    @property
    def oid(self):
        return hash((self.name))

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
        return self.name

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
            self.operations.push(OpExpr(op_obj=ColProjExpr(self, item),
                                        op_on=self,
                                        op_iter=False))

            return self

        if type(item) == IsInExpr:
            # self.operations.push(OpExpr(op_obj=item,
            #                             op_on=self,
            #                             op_iter=True))

            return self

        if isinstance(item, ColExtExpr):
            if item.func in [ExtFuncSymbol.StringContains,
                             ExtFuncSymbol.StartsWith,
                             ExtFuncSymbol.EndsWith]:

                self.operations.push(OpExpr(op_obj=item,
                                            op_on=self,
                                            op_iter=False))
                return self
            else:
                raise NotImplementedError(f'Unsupported external function {item.func}')

    def __getattr__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

    def get_col(self, col_name):
        """
        df['col_name'] = ?
        :param col_name:
        :return:
        """
        if col_name in self.columns:
            return ColEl(self, col_name)
        elif col_name in self.retriever.find_cols_used(mode='insert'):
            return ColEl(self, col_name)
        elif self.retriever.was_aggregated:
            if col_name in self.retriever.find_cols_used(mode='aggregation'):
                return ColEl(self, col_name)
        else:
            raise IndexError(f'Cannot find column "{col_name}" in {self.name}: {self.columns}')

    def __setitem__(self, key, value):
        if key in self.columns:
            if type(value) in (bool, int, float, str):
                return self.rename_col_scalar(key, value)
            if type(value) in (ColEl, ColOpExpr, CaseExpr, ColExtExpr):
                return self.rename_col_expr(key, value)
            if type(value) in (IfExpr,):
                return self.rename_col_expr(key, value)
        else:
            if type(value) in (bool, int, float, str):
                return self.insert_col_scalar(key, value)
            if type(value) in (ColEl, ColOpExpr, CaseExpr, ColExtExpr):
                return self.insert_col_expr(key, value)
            if type(value) in (IfExpr,):
                return self.insert_col_expr(key, value)

    def rename(self, mapper: dict, axis=1, inplace=True):
        for key in mapper.keys():
            if key in self.columns:
                self.__columns[self.__columns.index(key)] = mapper[key]
            else:
                raise IndexError(f'Cannot find the column {key} in {self.name}')

            self.operations.push(OpExpr(op_obj=OldColOpExpr(col_var=key,
                                                            col_expr=mapper[key]),
                                        op_on=self,
                                        op_iter=False))

    def rename_col_scalar(self, key, value):
        raise NotImplementedError

    def rename_col_expr(self, key, value):
        self.operations.push(OpExpr(op_obj=OldColOpExpr(col_var=key,
                                                        col_expr=value),
                                    op_on=self,
                                    op_iter=False))

    def insert_col_scalar(self, key, value):
        raise NotImplementedError

    def insert_col_expr(self, key, value):
        self.operations.push(OpExpr(op_obj=NewColOpExpr(col_var=key,
                                                        col_expr=value),
                                    op_on=self,
                                    op_iter=False))

    def groupby(self, cols, as_index=False):
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

        next_context_unopt = self.context_unopt + right.context_unopt

        next_context_semiopt = self.context_semiopt + right.context_semiopt

        next_cols = self.cols_out + right.cols_out

        tmp_name = f'{self.name}_{right.name}'

        tmp_df = DataFrame(name=tmp_name,
                           is_joint=True,
                           columns=next_cols,
                           context_variable=next_context_var,
                           context_constant=next_context_const,
                           context_unopt=next_context_unopt,
                           context_semiopt=next_context_semiopt)

        merge_expr = MergeExpr(left=self,
                               right=right,
                               how=how,
                               left_on=left_on,
                               right_on=right_on,
                               joint=tmp_df)

        self.push(OpExpr(op_obj=merge_expr,
                         op_on=[self, right],
                         op_iter=True))

        right.push(OpExpr(op_obj=merge_expr,
                          op_on=[self, right],
                          op_iter=True))

        tmp_df.push(OpExpr(op_obj=merge_expr,
                           op_on=[self, right],
                           op_iter=True))

        return tmp_df

    def get_opt(self, opt_goal=OptGoal.UnOptimized):
        opt = Optimizer(opt_on=self,
                        opt_goal=opt_goal)
        for op_expr in self.operations:
            opt.input(op_expr)
        return opt

    def agg(self, func=None, *agg_args, **agg_kwargs):
        if func:
            if type(func) == str:
                return self.agg_str_parse(func)
            if type(func) == dict:
                return self.agg_dict_parse(func)
        if agg_args:
            pass
        if agg_kwargs:
            return self.agg_kwargs_parse(agg_kwargs)

    def agg_dict_parse(self, input_aggr_dict):
        aggr_tuple_dict = {}
        for k in input_aggr_dict.keys():
            aggr_tuple_dict[k] = (k, input_aggr_dict[k])

        output_aggr_dict = {}

        for aggr_key in input_aggr_dict.keys():
            aggr_func = input_aggr_dict[aggr_key]

            if aggr_func == 'sum':
                output_aggr_dict[aggr_key] = self.key_access(aggr_key)
            if aggr_func == 'count':
                output_aggr_dict[aggr_key] = ConstantExpr(1)

        aggr_expr = AggrExpr(aggr_type=AggrType.Dict,
                             aggr_on=self,
                             aggr_op=output_aggr_dict,
                             aggr_else=ConstantExpr(None),
                             origin_dict=aggr_tuple_dict)

        op_expr = OpExpr(op_obj=aggr_expr,
                         op_on=self,
                         op_iter=True,
                         iter_on=self,
                         ret_type=OpRetType.DICT)

        self.push(op_expr)

        return self

    def agg_kwargs_parse(self, aggr_tuple_dict):
        agg_dict = {}

        for agg_key in aggr_tuple_dict.keys():
            agg_val = aggr_tuple_dict[agg_key]
            if not isinstance(agg_val, tuple):
                raise ValueError()

            agg_flag = aggr_tuple_dict[agg_key][1]

            if agg_flag == 'sum':
                agg_dict[agg_key] = self.key_access(agg_val[0])
            if agg_flag == 'count':
                agg_dict[agg_key] = ConstantExpr(1)
            if callable(agg_flag):
                # received lambda function
                agg_dict[agg_key] = ConstantExpr(1)

        aggr_expr = AggrExpr(aggr_type=AggrType.Dict,
                             aggr_on=self,
                             aggr_op=agg_dict,
                             aggr_else=EmptyDicConsExpr(),
                             origin_dict=aggr_tuple_dict)

        op_expr = OpExpr(op_obj=aggr_expr,
                         op_on=self,
                         op_iter=True,
                         iter_on=self,
                         ret_type=OpRetType.DICT)

        self.push(op_expr)

        return self

    def peak(self):
        return self.operations.peak()

    def show_info(self):
        if self.is_joint:
            self.partition_side.show_info()
            self.probe_side.show_info()

        print(f'>> {self.name} Columns <<')
        print(self.columns)
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
        print(f'========================================')

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

    @property
    def joint_frame(self):
        return self.get_opt(OptGoal.Joint).joint_frame

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
            if op_expr.op_type == GroupbyAggrExpr:
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
            if op_expr.op_type == NewColOpExpr:
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
                if isinstance(op_expr.op.left_on, str) and isinstance(op_expr.op.right_on, str):
                    cols_list.append((op_expr.op.left_on, op_expr.op.right_on))
                elif isinstance(op_expr.op.left_on, list) and isinstance(op_expr.op.right_on, list):
                    if len(op_expr.op.left_on) != len(op_expr.op.right_on):
                        raise ValueError('MergeError: left_on and right_on must be at the same length!')
                    for i in range(len(op_expr.op.left_on)):
                        l_on = op_expr.op.left_on[i]
                        r_on = op_expr.op.right_on[i]
                        cols_list.append((l_on, r_on))
                else:
                    raise NotImplementedError
                if self.name != op_expr.op.joint.name:
                    cols_list += op_expr.op.joint.find_cols_as_key_tuple()
        return list(set(cols_list))

    def get_name_ops(self):
        output = self.name
        for op_expr in self.operations:
            output += op_expr.get_op_name_suffix()
        return output

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
            if vname == 'lineitem':
                result += f"{vname} = VarExpr('db->li_dataset')\n"
            elif vname == 'customer':
                result += f"{vname} = VarExpr('db->cu_dataset')\n"
            elif vname == 'orders':
                result += f"{vname} = VarExpr('db->ord_dataset')\n"
            elif vname == 'nation':
                result += f"{vname} = VarExpr('db->na_dataset')\n"
            elif vname == 'region':
                result += f"{vname} = VarExpr('db->re_dataset')\n"
            elif vname == 'part':
                result += f"{vname} = VarExpr('db->pa_dataset')\n"
            elif vname == 'supplier':
                result += f"{vname} = VarExpr('db->su_dataset')\n"
            elif vname == 'partsupp':
                result += f"{vname} = VarExpr('db->ps_dataset')\n"
            else:
                result += f"{vname} = VarExpr('{vname}')\n"
        return result

    def define_constants(self):
        result_seq = VarBindSeq()
        for const in self.context_constant.keys():
            result_seq.push(VarBindExpr(var_expr=self.get_const_var(const),
                                        var_value=ConstantExpr(const)))
        return result_seq

    def get_aggr(self, next_op=None, as_part=False) -> LetExpr:
        return AggrFrame(self).get_aggr_expr(next_op, as_part)

    def get_groupby_aggr(self, next_op=None) -> LetExpr:
        return GroupbyAggrFrame(self).get_groupby_aggr_expr(next_op)

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
                self.context_unopt.append(LetExpr(tmp_var,
                                                  sum_expr,
                                                  ConstantExpr(True)))
            if op_expr.op_type == NewColOpExpr:
                sum_expr = SumExpr(iter_last_var,
                                   last_var,
                                   DicConsExpr([(ConcatExpr(iter_last_key,
                                                            RecConsExpr([(op_expr.op.col_var,
                                                                          op_expr.op.replace(iter_last_key))])
                                                            ),
                                                 ConstantExpr(True))]))
                self.context_unopt.append(LetExpr(tmp_var,
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
                self.context_unopt.append(LetExpr(tmp_var,
                                                  sum_expr,
                                                  ConstantExpr(True)))
            if op_expr.op_type == GroupbyAggrExpr:
                pass

            self.unopt_count += 1

        result = ''
        for i in range(self.unopt_count):
            result += f"v{i} = VarExpr('v{i}')\n"
            result += f"x_v{i} = VarExpr('x_v{i}')\n"
        result += f"out = VarExpr('out')\n"

        last_seq = VarBindSeq()
        for i in self.context_unopt:
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
        lamb_else = lamb_else.replace('else', '').replace(',', '').strip()

        if lamb_else == 'None':
            lamb_else = None
        elif lamb_else == '0':
            lamb_else = 0
        elif lamb_else == '0.0':
            lamb_else = 0.0
        else:
            if lamb_else.isdigit():
                lamb_else = int(lamb_else)
            if lamb_else.isdecimal():
                lamb_else = float(lamb_else)
            else:
                raise NotImplementedError(f'Unsupported Type {lamb_else}.')

        op = eval(lamb_op.replace(f'{lamb_arg}[', 'self['))
        if isinstance(op, (bool, int, float, str)):
            op = ConstantExpr(op)

        cond = eval(lamb_cond.replace(f'{lamb_arg}[', 'self['))

        if self.is_joint:
            if isinstance(cond, ColExtExpr):
                col_name = cond.col.field
                if col_name in self.partition_side.columns:
                    self.partition_side.push(OpExpr(op_obj=cond,
                                                    op_on=self,
                                                    op_iter=False))

                    return IfExpr(condExpr=CompareExpr(CompareSymbol.NE,
                                                       DicLookupExpr(self.joint_frame.part_frame.part_on_var,
                                                                     self.joint_frame.probe_frame.probe_key_sdql_ir),
                                                       ConstantExpr(None)),
                                  thenBodyExpr=op.sdql_ir,
                                  elseBodyExpr=ConstantExpr(lamb_else))
                elif col_name in self.probe_side.columns:

                    raise NotImplementedError
                elif col_name in self.columns:

                    raise NotImplementedError
                else:
                    raise IndexError(f'Cannot find column {col_name}')
            elif isinstance(cond, CondExpr):
                cond_on = self.retriever.find_cond_on(cond,
                                                      {self.partition_side.name: self.partition_side.cols_out,
                                                       self.probe_side.name: self.probe_side.columns,
                                                       self.name: self.cols_out})

                if len(cond_on) > 1 and self.name in cond_on:
                    cond_on.remove(self.name)

                if len(cond_on) == 1:
                    only_for = cond_on[0]

                    if only_for == self.partition_side.name:
                        apply_cond = cond.replace(rec=DicLookupExpr(self.joint_frame.part_frame.part_on_var,
                                                                    self.joint_frame.probe_frame.probe_key_sdql_ir),
                                                  inplace=False)

                        if isinstance(op, FlexIR):
                            apply_op = op.sdql_ir
                        else:
                            apply_op = op

                        return IfExpr(condExpr=apply_cond,
                                      thenBodyExpr=apply_op,
                                      elseBodyExpr=ConstantExpr(lamb_else))
                    elif only_for == self.probe_side.name:
                        apply_cond = cond.replace(rec=self.probe_side.iter_el.key,
                                                  inplace=False)

                        if isinstance(op, FlexIR):
                            apply_op = op.sdql_ir
                        else:
                            apply_op = op

                        return IfExpr(condExpr=apply_cond,
                                      thenBodyExpr=apply_op,
                                      elseBodyExpr=ConstantExpr(lamb_else))
                    elif only_for == self.name:
                        cols_in_cond = self.retriever.findall_cols_in_cond(cond)
                        if len(cols_in_cond) == 0:
                            raise NotImplementedError
                        elif len(cols_in_cond) == 1:
                            cols_inserted = self.retriever.findall_col_insert()
                            if cols_inserted:
                                new_col_name = cols_inserted[cols_in_cond[0]]

                                if isinstance(new_col_name, ColEl):
                                    old_col_name = cols_inserted[cols_in_cond[0]].field

                                    if old_col_name in self.partition_side.columns:
                                        apply_cond = cond.replace(
                                            rec=self.retriever.find_lookup_path(self, old_col_name),
                                            inplace=True)
                                    else:
                                        raise NotImplementedError
                                else:
                                    raise NotImplementedError
                            else:
                                raise NotImplementedError

                            if isinstance(op, ColEl):
                                cols_inserted = self.retriever.findall_col_insert()
                                if cols_inserted:
                                    new_col_op = cols_inserted[op.field]

                                    if isinstance(new_col_op, FlexIR):
                                        apply_op = op.replace(
                                            rec=new_col_op.sdql_ir,
                                            inplace=True)
                                    else:
                                        raise NotImplementedError
                                else:
                                    raise NotImplementedError
                            else:
                                raise NotImplementedError

                            return IfExpr(condExpr=apply_cond,
                                          thenBodyExpr=apply_op,
                                          elseBodyExpr=ConstantExpr(lamb_else))
                    else:
                        raise NotImplementedError
                else:
                    cond_mapper = {tuple(self.partition_side.cols_out):
                                       self.joint_frame.part_lookup(),
                                   tuple(self.probe_side.cols_out): self.probe_side.iter_el.key}

                    apply_cond = cond.replace(rec=None, inplace=False, mapper=cond_mapper)

                    if isinstance(op, FlexIR):
                        apply_op = op.sdql_ir
                    else:
                        apply_op = op

                    return IfExpr(condExpr=self.joint_frame.part_nonull(),
                                  thenBodyExpr=IfExpr(condExpr=apply_cond,
                                                      thenBodyExpr=apply_op,
                                                      elseBodyExpr=ConstantExpr(lamb_else)),
                                  elseBodyExpr=ConstantExpr(lamb_else))
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    def optimize_obj(self):
        opt = self.get_opt()
        for op_expr in self.operations:
            opt.input(op_expr)

        return opt.output

    def get_history(self):
        return self.operations

    def get_retriever(self) -> Retriever:
        return self.__retriever

    @property
    def retriever(self) -> Retriever:
        return self.__retriever

    def drop_duplicates(self):
        return self

    def squeeze(self):
        return self
