class ColElAttach:
    def __init__(self, col_from, col_to):
        """
        DataFrame.__setitem__
        DataFrame.insert_col_expr

        :param col_from: ColEl the column is created from
        :param col_to: ColEl the column is attached to
        """
        self.col_from = col_from
        self.col_to = col_to

        self.create_from = col_from.relation
        self.attach_to = col_to.relation


    @property
    def op_suffix_name(self):
        return  f'_attach_other_col'