import string

from pysdql.core.dtypes.IterationElement import IterEl


class IterExpr:
    def __init__(self, r_name, key=None, val=None):
        self.r_name = r_name
        self.given_key = key
        self.given_val = val

    @property
    def n_e_dict(self):
        d = {"tmp": "t", "tmpa": "ta", "tmpb": "tb", "tmpc": "tc", "tmpd": "td", "tmpe": "te", "tmpf": "tf",
             "tmpg": "tg", "tmph": "th", "tmpi": "ti", "tmpj": "tj", "tmpk": "tk", "tmpl": "tl",
             "tmpm": "tm", "tmpn": "tn", "tmpo": "to", "tmpp": "tp", "tmpq": "tq", "tmpr": "tr",
             "tmps": "ts", "tmpt": "tt", "tmpu": "tu", "tmpv": "tv", "tmpw": "tw", "tmpx": "tx",
             "tmpy": "ty", "tmpz": "tz"}
        return d

    @staticmethod
    def hard_code_n_e_dict():
        tmp_list = []
        for i in list(string.ascii_lowercase):
            tmp_list.append(f'"tmp{i}": "t{i}"')
        tmp_str = ', '.join(tmp_list)
        print(f'{{ {tmp_str} }}')

    @property
    def element(self):
        return self.gen_iter_el()

    @property
    def name(self):
        return self.element.name

    @property
    def key(self):
        if self.given_key:
            return self.given_key
        return self.element.key

    @property
    def val(self):
        if self.given_val:
            return self.given_val
        return self.element.val

    @property
    def kv_pair(self):
        if self.given_key and self.given_val:
            return f'<{self.given_key}, {self.given_val}>'
        return f'<{self.key}, {self.val}>'

    def gen_iter_el(self) -> IterEl:

        if self.r_name in self.n_e_dict.keys():
            tmp_name = self.n_e_dict[self.r_name]
            return IterEl((f'{tmp_name}_k', f'{tmp_name}_v'))

        if any(c.isdigit() for c in self.r_name):
            tmp_name = str(self.r_name[0]).lower()
            for i in self.r_name:
                if i.isdigit():
                    return IterEl((f'{tmp_name}{i}_k', f'{tmp_name}{i}_v'))

        special_char = '_'
        tmp_name = ''
        for i in special_char:
            if i in self.r_name:
                tmp_list = str(self.r_name).split('_')
                for j in tmp_list:
                    tmp_name += j[0]
                return IterEl((f'{tmp_name}_k', f'{tmp_name}_v'))

        tmp_name = str(self.r_name[0]).lower()
        return IterEl((f'{tmp_name}_k', f'{tmp_name}_v'))

    @property
    def expr(self):
        if self.given_key and self.given_val:
            return f'sum (<{self.given_key}, {self.given_val}> in {self.r_name})'
        return f'sum ({self.element.expr} in {self.r_name})'

    def __repr__(self):
        return self.expr

    def merge(self, other):
        return f'{self} {other}'
