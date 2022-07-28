import string

from pysdql.core.dtypes.ColEl import ColEl
from pysdql.core.dtypes.CondExpr import CondExpr
from pysdql.core.dtypes.GroupbyExpr import GroupbyExpr
from pysdql.core.dtypes.OpExpr import OpExpr
from pysdql.core.dtypes.VarExpr import VarExpr
from pysdql.core.dtypes.IterStmt import IterStmt
from pysdql.core.dtypes.CondStmt import CondStmt
from pysdql.core.dtypes.IterExpr import IterExpr
from pysdql.core.dtypes.DictEl import DictEl
from pysdql.core.dtypes.RecEl import RecEl


class JoinExpr:
    def __init__(self, left, right, how, left_key, right_key, more_cond=None):
        self.left = left
        self.left_col = left.cols
        self.left_key = left_key
        self.right = right
        self.right_col = right.cols
        self.right_key = right_key
        self.how = how
        self.more_cond = more_cond

        # from pysdql.core.dtypes.relation import relation
        # next_name = left.gen_tmp_name(noname=left.history_name + right.history_name)
        # jdict = relation(name=next_name, inherit_from=left).inherit(right)
        # self.next_relation = jdict

        self.name = f'{how}_join_{left.name[0]}_{right.name[0]}'
        self.iter_expr = IterExpr(self.name)
        self.history_name = self.left.history_name + self.right.history_name
        self.operations = self.right.operations + self.left.operations

        if self.how == 'left':
            self.joint_dict = self.get_left_joint_dict()
        elif self.how == 'right':
            self.joint_dict = self.get_right_joint_dict()
        elif self.how == 'inner':
            self.joint_dict = self.get_inner_joint_dict()
        elif self.how == 'full':
            self.joint_dict = self.get_full_joint_dict()
        elif self.how == 'cross':
            self.joint_dict = self.get_cross_joint_dict()

    def gen_tmp_name(self, noname=None):
        if noname is None:
            noname = []
        name_list = [f'{str(self.name[0]).lower()}mp', 'tmp']
        for i in list(string.ascii_lowercase):
            name_list.append(f'tmp{i}')

        dup_list = [self.name] + self.history_name + noname

        for tmp_name in name_list:
            if tmp_name not in dup_list:
                return tmp_name

    def get_left_joint_dict(self):
        right_group_name = f'{self.right.name}_right_group'
        right_group_iter_expr = IterExpr(right_group_name)
        if self.more_cond:
            right_group_dict = None
            # right_group_dict = VarExpr(right_group_name, IterStmt(self.right.iter_expr, CondStmt(self.more_cond, DictEl(
            #     {self.right.iter_expr.key: DictEl({self.right.iter_expr.key: self.right.iter_expr.val})}), DictEl({}))))
        else:
            right_group_dict = VarExpr(right_group_name, IterStmt(self.right.iter_expr, DictEl(
                {f'<{self.right_key}={self.right.iter_expr.key}.{self.right_key}>': DictEl(
                    {self.right.iter_expr.key: self.right.iter_expr.val})})))

        self.history_name.append(right_group_name)
        self.operations.append(OpExpr('joinexpr_get_left_joint_dict_right_group_dict', right_group_dict))

        # left_group_name = f'{self.left.name}_left_group'
        left_group_name = self.name
        right_group_nested_dict = f'{right_group_name}(<{self.right_key}={self.left.iter_expr.key}.{self.left_key}>)'
        right_group_nested_dict_key = f'rgnd_{self.right.name[0]}_k'
        right_group_nested_dict_val = f'rgnd_{self.right.name[0]}_v'
        right_group_nested_dict_iter_expr = f'sum(<{right_group_nested_dict_key}, {right_group_nested_dict_val}> in {right_group_nested_dict})'

        if self.more_cond:
            left_group_dict = None
        else:

            tmp_cond = CondStmt(CondExpr(right_group_nested_dict, '!=', DictEl({})),
                                IterStmt(right_group_nested_dict_iter_expr, DictEl({RecEl(
                                    {'left': self.left.iter_expr.key,
                                     'right': right_group_nested_dict_key}): 1})), # f'{self.left.iter_expr.val} * {right_group_nested_dict_val}'
                                DictEl({RecEl(
                                    {'left': self.left.iter_expr.key, 'right': RecEl({})}): 1})) # self.left.iter_expr.val
            left_group_dict = VarExpr(left_group_name, IterStmt(self.left.iter_expr, tmp_cond))

            self.history_name.append(left_group_name)
            self.operations.append(
                OpExpr('joinexpr_get_left_joint_dict_left_group_dict', left_group_dict))

        return self

    def get_right_joint_dict(self):
        return self

    def get_inner_joint_dict(self):
        return self

    def get_full_joint_dict(self):
        return self

    def get_cross_joint_dict(self):
        return self

    def groupby(self, cols):
        if type(cols) != list:
            raise TypeError()

        var_name = self.gen_tmp_name()
        return GroupbyExpr(name=var_name,
                           groupby_from=self,
                           groupby_cols=cols)

    def __getitem__(self, item):
        if type(item) == str:
            return self.get_col(col_name=item)

    @property
    def key(self):
        return self.iter_expr.key

    @property
    def val(self):
        return self.iter_expr.val

    @property
    def left_field(self):
        return f'{self.iter_expr.key}.left'

    @property
    def right_field(self):
        return f'{self.iter_expr.key}.right'

    def get_col(self, col_name):
        return ColEl(self, col_name)

    @property
    def sdql_expr(self):
        expr_str = f'\n'.join([f'{i}' for i in self.operations])
        expr_str += f'\n{self.name}'
        return expr_str
