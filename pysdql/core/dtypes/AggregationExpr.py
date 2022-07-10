class AggrExpr:
    def __init__(self, relation, column: str, aggr_func: str):
        self.relation = relation
        self.element = relation.iter_el
        self.col = column
        self.aggr_func = aggr_func

    @property
    def expr(self) -> str:
        if self.aggr_func == 'sum':
            return f'let {self.relation.gen_tmp_name()} = sum({self.element} in {self.relation.r_name}) {{ "{self.col}" -> {self.element.key}.{self.col} * {self.element.val} }}\n' + f'in sum (y in {self.relation.gen_tmp_name()}) {{ <{self.col}=y.val> }}'
        if self.aggr_func == 'count':
            return ''
        if self.aggr_func == 'avg':
            return ''

    def __repr__(self):
        return self.expr
