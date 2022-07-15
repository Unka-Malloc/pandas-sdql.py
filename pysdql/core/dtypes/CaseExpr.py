class CaseExpr:
    def __init__(self, when, then_case, else_case):
        self.when = when
        self.then_case = then_case
        self.else_case = else_case

    def set(self, col_name, r_name, iter_expr):
        print(f'let {r_name} = {iter_expr} '
              f'if ({self.when}) '
              f'then {{ concat({iter_expr.key}, <{col_name}={self.then_case}>) }} '
              f'else {{ concat({iter_expr.key}, <{col_name}={self.else_case}>) }} in')

