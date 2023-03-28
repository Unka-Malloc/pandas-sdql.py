class AddColProj:
    def __init__(self, add_col):
        """

        :param add_col: list additional columns
        """
        self.add_col = add_col

    @property
    def op_suffix_name(self):
        return f'additional_columns'