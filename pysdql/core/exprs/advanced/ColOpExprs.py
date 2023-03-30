from pysdql.core.utils.format_utils import (
    input_fmt,
)

from pysdql.core.interfaces.identifier.api import (
    IgnoreThisFlag,
)

from pysdql.core.interfaces.availability.api import (
    Replaceable,
)

from pysdql.core.exprs.advanced.BinCondExpr import (
    BinCondExpr,
)

from pysdql.core.exprs.complex.api import (
    AggrExpr,
)

from pysdql.core.enums.EnumUtil import (
    MathSymbol,
    LogicSymbol,
    AggrType,
    OpRetType,
)

from pysdql.core.exprs.carrier.OpExpr import OpExpr

from pysdql.core.killer.SDQLInspector import SDQLInspector

from pysdql.core.prototype.basic.sdql_ir import (
    Expr,
    ConstantExpr,
    AddExpr,
    MulExpr,
    SubExpr,
    DivExpr,
    CompareSymbol,
    CompareExpr,
    IfExpr,
    ExtFuncSymbol, ExtFuncExpr
)

"""
ColOpBinary -> Binary Arithmetic Operation For Columns (+, -, *, /)
ColOpExternal -> | str.contains()
            | str.find()
            | str.startswith()
            | str.endswith()
            | str.slice()
ColOpApply -> DataFrame.apply()
ColOpIsNull -> Series.isnull()
ColOpIsin -> Series.isin()
"""


class ColOpBinary(Replaceable):
    def __init__(self, unit1, operator, unit2):
        self.unit1 = unit1
        self.operator = operator
        self.unit2 = unit2

    @property
    def relation(self):
        if isinstance(self.unit1, (bool, int, float)):
            return self.unit2.relation
        elif isinstance(self.unit2, (bool, int, float)):
            return self.unit1.relation
        else:
            if self.unit1.relation.oid == self.unit2.relation.oid:
                return self.unit1.relation

    def sum(self):
        aggr_expr = AggrExpr(aggr_type=AggrType.Scalar,
                             aggr_on=self.relation,
                             aggr_op={f'sum_agg': self.sdql_ir},
                             aggr_else=ConstantExpr(0.0),
                             origin_dict={f'sum_agg': (self.sdql_ir, 'sum')},
                             unique_columns=self.relation.retriever.find_cols(self),
                             is_multi_col_op=True)

        op_expr = OpExpr(op_obj=aggr_expr,
                         op_on=self.relation,
                         op_iter=True,
                         iter_on=self.relation,
                         ret_type=OpRetType.FLOAT)

        self.relation.push(op_expr)

        return aggr_expr

    '''
    Arithmetic Operations
    '''

    def __add__(self, other):
        return ColOpBinary(unit1=self,
                           operator=MathSymbol.ADD,
                           unit2=other)
        # return ColExpr(value=AddExpr(self.col, input_fmt(other)), relation=self.relation)

    def __mul__(self, other):
        return ColOpBinary(unit1=self,
                           operator=MathSymbol.MUL,
                           unit2=other)
        # return ColExpr(value=MulExpr(self.col, input_fmt(other)), relation=self.relation)

    def __sub__(self, other):
        return ColOpBinary(unit1=self,
                           operator=MathSymbol.SUB,
                           unit2=other)
        # return ColExpr(value=SubExpr(self.col, input_fmt(other)), relation=self.relation)

    def __truediv__(self, other):
        return ColOpBinary(unit1=self,
                           operator=MathSymbol.DIV,
                           unit2=other)
        # return ColExpr(value=DivExpr(self.col, input_fmt(other)), relation=self.relation)

    '''
    Reverse Arithmetic Operations
    '''

    def __radd__(self, other):
        return ColOpBinary(unit1=other,
                           operator=MathSymbol.ADD,
                           unit2=self)
        # return ColExpr(value=AddExpr(input_fmt(other), self.col), relation=self.relation)

    def __rmul__(self, other):
        return ColOpBinary(unit1=other,
                           operator=MathSymbol.MUL,
                           unit2=self)
        # return ColExpr(value=MulExpr(input_fmt(other), self.col), relation=self.relation)

    def __rsub__(self, other):
        return ColOpBinary(unit1=other,
                           operator=MathSymbol.SUB,
                           unit2=self)
        # return ColExpr(value=SubExpr(input_fmt(other), self.col), relation=self.relation)

    def __rtruediv__(self, other):
        return ColOpBinary(unit1=other,
                           operator=MathSymbol.DIV,
                           unit2=self)
        # return ColExpr(value=DivExpr(input_fmt(other), self.col), relation=self.relation)

    def replace(self, rec, inplace=False, mapper=None):
        new_unit1 = self.unit1
        new_unit2 = self.unit2

        if isinstance(self.unit1, Replaceable):
            new_unit1 = self.unit1.replace(rec, inplace, mapper)
        if isinstance(self.unit2, Replaceable):
            new_unit2 = self.unit2.replace(rec, inplace, mapper)

        if self.operator == MathSymbol.ADD:
            return AddExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.MUL:
            return MulExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.SUB:
            return SubExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        if self.operator == MathSymbol.DIV:
            return DivExpr(op1Expr=input_fmt(new_unit1),
                           op2Expr=input_fmt(new_unit2))
        raise NotImplemented

    @staticmethod
    def unit_fmt(value):
        if isinstance(value, (bool, int, float, str)):
            return value
        elif isinstance(value, (Replaceable)):
            return value.oid
        else:
            return hash(value)

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return True

    @property
    def oid(self):
        return hash((
            self.operator,
            self.unit_fmt(self.unit1),
            self.unit_fmt(self.unit2),
        ))

    @property
    def sdql_ir(self):
        if self.operator == MathSymbol.ADD:
            return AddExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.MUL:
            return MulExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.SUB:
            return SubExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        if self.operator == MathSymbol.DIV:
            return DivExpr(op1Expr=input_fmt(self.unit1),
                           op2Expr=input_fmt(self.unit2))
        raise NotImplemented

    def __repr__(self):
        return repr(self.sdql_ir)

    @property
    def descriptor(self):
        return SDQLInspector.find_a_descriptor(self.sdql_ir)


class ColOpExternal(Replaceable):
    def __init__(self, col, ext_func, args=None, isinvert=False, is_apply_cond=False):
        """

        :param ColEl col:
        :param ext_func:
        :param args:
        :param isinvert:
        """
        self.col = col
        self.func = ext_func
        self.args = args

        self.vars = {}

        self.isinvert = isinvert

        self.is_apply_cond = is_apply_cond

    def replace(self, rec, inplace=False, mapper=None):
        if isinstance(self.col, Replaceable):
            return ColOpExternal(self.col.replace(rec, inplace, mapper), self.func, self.args, self.isinvert)

        raise NotImplementedError(f'Unexpected type {self.col}: {self.col}')


    def gen_cond_expr(self, operator, unit2):
        """
        :param operator: CompareSymbol
        :param unit2: ColEl | (float, int, str) | date@str
        :return:
        """
        if operator == CompareSymbol.EQ:
            if type(unit2) == str:
                self.col.add_const(unit2)
                return BinCondExpr(unit1=self, operator=operator, unit2=self.col.get_const_var(unit2))
            return BinCondExpr(unit1=self,
                               operator=operator,
                               unit2=unit2)

        return BinCondExpr(unit1=self,
                           operator=operator,
                           unit2=unit2)

    def __eq__(self, other) -> BinCondExpr:
        """
        Equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.EQ,
                                  unit2=other)
        # if type(other) == str:
        #     self.add_const(other)
        #     return CondExpr(unit1=self.col, operator=CompareSymbol.EQ, unit2=self.get_const_var(other))
        # return CondExpr(unit1=self.col, operator=CompareSymbol.EQ, unit2=input_fmt(other))

    def __ne__(self, other) -> BinCondExpr:
        """
        Not equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.NE,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.NE, unit2=input_fmt(other))

    def __lt__(self, other) -> BinCondExpr:
        """
        Less than
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.LT,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.LT, unit2=input_fmt(other))

    def __le__(self, other) -> BinCondExpr:
        """
        Less than or Equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.LTE,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.LTE, unit2=input_fmt(other))

    def __gt__(self, other) -> BinCondExpr:
        """
        Greater than
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.GT,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.GT, unit2=input_fmt(other))

    def __ge__(self, other) -> BinCondExpr:
        """
        Greater than or Equal
        :param other:
        :return:
        """
        return self.gen_cond_expr(operator=CompareSymbol.GTE,
                                  unit2=other)
        # return CondExpr(unit1=self.col, operator=CompareSymbol.GTE, unit2=input_fmt(other))

    def __and__(self, other):
        return BinCondExpr(unit1=self,
                           operator=LogicSymbol.AND,
                           unit2=other)

    def __or__(self, other):
        return BinCondExpr(unit1=self,
                           operator=LogicSymbol.OR,
                           unit2=other)

    def __invert__(self):
        return BinCondExpr(unit1=self,
                           operator=LogicSymbol.NOT,
                           unit2=self)

    def __add__(self, other):
        return ColOpBinary(unit1=self,
                           operator=MathSymbol.ADD,
                           unit2=other)

    '''
    FlexIR
    '''

    @property
    def replaceable(self):
        return True

    @property
    def oid(self):
        return hash((
            self.col.oid,
            self.func,
            self.args
        ))

    @property
    def sdql_ir(self):
        if isinstance(self.col, Replaceable):
            col_expr = self.col.sdql_ir
        elif isinstance(self.col, Expr):
            col_expr = self.col
        else:
            raise TypeError(f'Illegal Column IR {type(self.col)} {self.col}')

        if self.func == ExtFuncSymbol.StartsWith:
            return ExtFuncExpr(self.func,
                               col_expr,
                               self.args,
                               ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.EndsWith:
            return ExtFuncExpr(self.func,
                               col_expr,
                               self.args,
                               ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.StringContains:
            return CompareExpr(CompareSymbol.NE,
                               ExtFuncExpr(ExtFuncSymbol.FirstIndex,
                                           col_expr,
                                           self.args,
                                           ConstantExpr("Nothing!")),
                               MulExpr(ConstantExpr(-1), ConstantExpr(1)))

        if self.func == ExtFuncSymbol.FirstIndex:
            return ExtFuncExpr(self.func,
                                col_expr,
                                self.args,
                                ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.ExtractYear:
            return ExtFuncExpr(self.func,
                                col_expr,
                                ConstantExpr("Nothing!"),
                                ConstantExpr("Nothing!"))

        if self.func == ExtFuncSymbol.SubStr:
            return ExtFuncExpr(self.func,
                                col_expr,
                                ConstantExpr(int(self.args[0])),
                                ConstantExpr(int(self.args[1]) - 1))

        raise NotImplementedError(f'''
        {self.col},
        {self.func}
        ''')

    def __repr__(self):
        return str(self.sdql_ir)

    def isin(self, vals):
        if type(vals) == list or type(vals) == tuple:
            if len(vals) == 0:
                raise ValueError()
            if len(vals) == 1:
                return vals[0]

            tmp_list = []
            for i in vals:
                tmp_list.append(self.gen_cond_expr(operator=CompareSymbol.EQ,
                                                   unit2=i))

            a = tmp_list.pop()
            b = tmp_list.pop()

            tmp_cond = a | b

            if tmp_list:
                for i in tmp_list:
                    tmp_cond |= i

            return tmp_cond


class ColOpApply:
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


class ColOpIsNull(IgnoreThisFlag):
    def __init__(self, col):
        self.col = col
        self.is_invert = False

    def __invert__(self):
        self.is_invert = True

        return self
