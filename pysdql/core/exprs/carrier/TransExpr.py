from pysdql.core.enums.EnumUtil import OpRetType


class TransExpr:
    def __init__(self, trans_from):
        self.__trans_from = trans_from

        self.__trans_to = None

        self.__trans_to_var = None

        # OpRetType
        self.__trans_to_struct = None

        self.__scal = False

        self.__dict_keys = ()
        self.__dict_vals = ()

        self.__dict_key_types = ()
        self.__dict_val_types = ()

        self.__rec_keys = ()
        self.__rec_key_types = ()

    @property
    def trans_from(self):
        return self.__trans_from

    def migrate(self, trans: dict):
        """
        struct_index must be a tuple with two tuples. ((key_tuple), (val_tuple))
        :param trans:
        :return:
        """
        refs = ('to', 'to_var', 'to_struct', 'struct_index', 'struct_types')
        if not all(elem in refs for elem in list(trans.keys())):
            if not isinstance(trans['to_struct'], OpRetType):
                raise TypeError
            raise ValueError

        self.__trans_to = trans['to']
        self.__trans_to_var = trans['to_var']
        self.__trans_to_struct = trans['to_struct']

        if trans['to_struct'] == OpRetType.DICT:
            if not isinstance(trans['struct_index'], tuple) or not len(trans['struct_index']) == 2:
                raise ValueError

            if not all(isinstance(i, str) for i in trans['struct_index'][0]) \
                    or not all(isinstance(i, str) for i in trans['struct_index'][1]):
                raise TypeError

            self.__dict_keys = trans['struct_index'][0]
            self.__dict_vals = trans['struct_index'][1]

            if trans['struct_types']:
                if not isinstance(trans['struct_types'], tuple) or not len(trans['struct_types']) == 2:
                    raise ValueError

                if len(trans['struct_index'][0]) != len(trans['struct_types'][0]) \
                        or len(trans['struct_index'][1]) != len(trans['struct_types'][1]):
                    raise ValueError

                if not all(isinstance(t, OpRetType) for t in trans['struct_types'][0]) \
                        or not all(isinstance(t, OpRetType) for t in trans['struct_types'][1]):
                    raise TypeError

                self.__dict_key_types = trans['struct_types'][0]
                self.__dict_val_types = trans['struct_types'][1]

        if trans['to_struct'] == OpRetType.RECORD:
            if not all(isinstance(i, OpRetType) for i in trans['struct_index']):
                raise TypeError

            self.__rec_keys = trans['struct_index']

            if trans['struct_types']:
                if len(trans['struct_index']) != len(trans['struct_types']):
                    raise ValueError

                if not all(isinstance(t, OpRetType) for t in trans['struct_types']):
                    raise TypeError

                self.__rec_key_types = trans['struct_types']

        if trans['to_struct'] in [OpRetType.BOOL, OpRetType.INT, OpRetType.FLOAT, OpRetType.STRING]:
            self.__scal = True
