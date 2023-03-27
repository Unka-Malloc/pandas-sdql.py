from pysdql.core.dtypes.AggrNunique import AggrNunique


class AggrLambda:
    def __init__(self, relation, field):
        self.field = field
        self.relation = relation

    def nunique(self):
        return AggrNunique(self)