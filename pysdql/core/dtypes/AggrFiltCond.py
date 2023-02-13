class AggrFiltCond():
    def __init__(self, aggr_unit1, aggr_unit2, cond_op):
        self.aggr_unit1 = aggr_unit1
        self.aggr_unit2 = aggr_unit2
        self.cond_op = cond_op

    @property
    def op_name_suffix(self):
        return '_filter'

    def __repr__(self):
        return f'{self.aggr_unit1}\n{self.cond_op}\n{self.aggr_unit2}'
