import string

from pysdql.core.dtypes.IterEl import IterEl


class IterExpr:
    def __init__(self, name, key=None, val=None):
        self.__name = name
        self.__key = key
        self.__val = val

    @property
    def ref_dict(self):
        d = {"tmp": "t", "tmpa": "ta", "tmpb": "tb", "tmpc": "tc", "tmpd": "td", "tmpe": "te", "tmpf": "tf",
             "tmpg": "tg", "tmph": "th", "tmpi": "ti", "tmpj": "tj", "tmpk": "tk", "tmpl": "tl",
             "tmpm": "tm", "tmpn": "tn", "tmpo": "to", "tmpp": "tp", "tmpq": "tq", "tmpr": "tr",
             "tmps": "ts", "tmpt": "tt", "tmpu": "tu", "tmpv": "tv", "tmpw": "tw", "tmpx": "tx",
             "tmpy": "ty", "tmpz": "tz",
             "lineitem": "lin", "part": "par", "supplier": "sup", "partsupp": "psup",
             "customer": "cus", "orders": "ord", "nation": "nat", "region": "reg"}
        return d

    @staticmethod
    def hard_code_ref_dict():
        tmp_list = []
        for i in list(string.ascii_lowercase):
            tmp_list.append(f'"tmp{i}": "t{i}"')
        tmp_str = ', '.join(tmp_list)
        print(f'{{ {tmp_str} }}')

    @property
    def iter_el(self):
        if self.__key and self.__val:
            return IterEl((self.__key, self.__val))

        if self.__name in self.ref_dict.keys():
            tmp_name = self.ref_dict[self.__name]
        else:
            tmp_name = str(self.__name[0]).lower()

        sp_char = '_'
        short_tmp_name = ''
        for i in sp_char:
            if i in self.__name:
                tmp_list = str(self.__name).split('_')
                for j in tmp_list:
                    if j.isdigit():
                        continue
                    short_tmp_name += j[0]
        tmp_name += short_tmp_name[1:]

        short_tmp_name = ''
        if any(c.isdigit() for c in self.__name):
            for i in self.__name:
                if i.isdigit():
                    short_tmp_name += i
        tmp_name += short_tmp_name

        return IterEl((f'{tmp_name}_k', f'{tmp_name}_v'))

    @property
    def key(self):
        return self.__key

    @property
    def val(self):
        return self.__val

    @property
    def expr(self):
        if self.__key and self.__val:
            return f'sum (<{self.__key}, {self.__val}> in {self.__name})'
        return f'sum ({self.iter_el} in {self.__name})'

    def __repr__(self):
        return self.expr
