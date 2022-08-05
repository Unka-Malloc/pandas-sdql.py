class DataFrameStruct:
    def __init__(self, type):
        if type in ('1DT', 'LRT', 'GRP'):
            self.type = type
        else:
            raise ValueError(f'Unacceptable structure {type}')

    def __repr__(self):
        return self.type
