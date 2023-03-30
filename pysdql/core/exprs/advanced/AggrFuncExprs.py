class AggrLambda:
    def __init__(self, relation, field):
        self.field = field
        self.relation = relation

    def nunique(self):
        return AggrNuniqueFunc(self)

class AggrNuniqueFunc:
    def __init__(self, create_from):
        self.create_from = create_from

    @property
    def relation(self):
        return self.create_from.relation

    @property
    def field(self):
        return self.create_from.field

    @property
    def col(self):
        return self.relation[self.field]
