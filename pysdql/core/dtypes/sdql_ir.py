from typing import List, Tuple
from enum import Enum


class SemiRing:

    def __init__(self):
        pass

    def __eq__(self, other):
        if self is None and other is None:
            return True
        if self is None or other is None:
            return False
        if self.value == other.value:
            return True
        return False

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        isnull_self = self == None
        isnull_other = other == None
        if isnull_self and isnull_other:
            return False
        if isnull_self or isnull_other:
            return False
        if self.value < other.col:
            return True
        return False

    def __le__(self, other):
        if self < other or self == other:
            return True
        return False

    def __gt__(self, other):
        if not (self <= other):
            return True
        return False

    def __ge__(self, other):
        if not (self < other):
            return True
        return False

    def __hash__(self):
        return hash((str(self)))

    def __len__(self):
        if type(self) == SemiRingBool:
            return self.value
        return len(self.value)

    # def __neg__(self):
    #     if type(self) == SemiRingFloat:
    #         return SemiRingFloat(-0.01*(self.value))
    #     elif type(self) == SemiRingInt:
    #         return SemiRingInt(-1*(self.value))


class SemiRingDictionary(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, key_type=SemiRing, value_type=SemiRing, initialvalue=None):
        SemiRing.__init__(self)
        self.__key_type = key_type
        self.__value_type = value_type
        if initialvalue is not None:
            self.__value = dict(initialvalue)
        else:
            self.__value = dict()

    def get_type(self):
        return (self.__key_type, self.__value_type)

    def __str__(self):
        if (len(self.value.items()) == 0):
            return "{}"
        res = "{ "
        for (k, v) in self.value.items():
            if type(k) == str:
                res += "\"" + k + "\""
            else:
                res += str(k)
            res += " -> "
            if type(v) == str:
                res += "\"" + v + "\""
            else:
                res += str(v)
            res += ", "
        return res[:len(res) - 2:] + "}"

    def __add__(self, other):
        if (self.get_type() == (SemiRing, SemiRing)) or (other.get_type() == (SemiRing, SemiRing)) or (
                self.get_type() == other.get_type()):
            if type(other) == SemiRingDictionary or type(other) == SemiRingSet:
                res = SemiRingDictionary(
                    self.get_type()[0], self.get_type()[1])
                for (k, v) in self.value.items():
                    res[k] = v + \
                             other.col.get(k, zero(v))
                for k in other.col:
                    if k not in res.value:
                        res[k] = other[k] + \
                                 zero(other[k])
                return res
            else:
                raise ValueError('SemiRingDictionary + ' +
                                 str(type(other)) + " is not allowed!")
        else:
            raise ValueError("{" + str(self.get_type()[0]) + " -> " + str(self.get_type()[1]) + "} + {" + str(
                other.get_type()[0]) + " -> " + str(other.get_type()[1]) + "} is not supported!")

    def __mul__(self, other):
        res = None
        for (k, v) in self.value.items():
            tmp = v * other
            if res == None:
                res = SemiRingDictionary(self.get_type()[0], type(tmp))
            res[k] = tmp
        if res == None:
            res = SemiRingDictionary(self.get_type()[0], self.get_type()[1])
        return res

    def __getitem__(self, key):
        tmpvalue = self.value.get(key)
        if tmpvalue != None:
            return tmpvalue
        else:
            return zero(self.get_type()[1])

    def __setitem__(self, key, newvalue):
        if self.get_type() == (SemiRing, SemiRing):
            self.__key_type = type(key)
            self.__value_type = type(newvalue)

        # if (type(key) != self.get_type()[0] or type(newvalue) != self.get_type()[1]):
        #     raise TypeError("key/value type is not match to dictionary type.")
        self.__value[key] = newvalue


class SemiRingRecord(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue is not None:
            self.__value = dict(initialvalue)
        else:
            self.__value = dict()

    def __str__(self):
        if (len(self.value.items()) == 0):
            return "<>"
        else:
            res = "< "
            for (k, v) in self.value.items():
                res += str(k) + " = " + str(v) + ", "
            res = res[:len(res) - 2:] + " >"
        return res

    def __add__(self, other):
        if type(other) == SemiRingRecord and sorted(self.value.keys()) == sorted(other.col.keys()):
            pairs = {}
            if len(self.value.items()) > 0:
                for (k, v) in self.value.items():
                    pairs[k] = v + other.col[k]
                return SemiRingRecord(pairs)
            return SemiRingRecord()
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        pairs = {}
        for (k, v) in self.value.items():
            pairs[k] = v * other
        return SemiRingRecord(pairs)

    def __getitem__(self, key):
        columnvalue = self.value.get(key)
        if columnvalue != None:
            return columnvalue
        else:
            return None


class SemiRingSet(SemiRingDictionary):
    def __init__(self, key_type=None, initialvalue=None):
        SemiRingDictionary.__init__(self, key_type, SemiRingBool)
        zerobool = SemiRingBool(True)
        if initialvalue != None:
            for item in initialvalue:
                self.value[item] = zerobool


class SemiRingFloat(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = 0
        elif not isinstance(initialvalue, float):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)

    def __add__(self, other):
        if type(other) == SemiRingFloat:
            res = float(self.value + other.col)
            return SemiRingFloat(res)
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        if type(other) == SemiRingFloat:
            res = self.value * other.col
            return SemiRingFloat(float(res))
        elif type(other) == SemiRingDictionary or type(other) == SemiRingRecord or type(other) == SemiRingSet:
            res = None
            if type(other) == SemiRingDictionary:
                res = SemiRingDictionary(
                    other.get_type()[0], other.get_type()[1])
            elif type(other) == SemiRingSet:
                res = SemiRingSet(other.get_type()[0], None)
            else:
                res = {}

            for (k, v) in other.col.items():
                res[k] = self * v

            if type(other) == SemiRingRecord:
                return SemiRingRecord(res)
            return res
        else:
            raise ValueError(str(type(self)) + " * " +
                             str(type(other)) + " is not allowed!")

    def __truediv__(self, other):
        if type(other) == SemiRingFloat:
            res = self.value / other.col
            return SemiRingFloat(float(res))
        else:
            raise ValueError(str(type(self)) + " / " +
                             str(type(other)) + " is not allowed!")

    def __sub__(self, other):
        if self != None and other != None and type(other) == type(self):
            return type(self)(self.value - other.col)
        else:
            raise ValueError("subtraction cannot be done.")


class SemiRingInt(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = 0
        elif not isinstance(initialvalue, int):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)

    def __add__(self, other):
        if type(other) == SemiRingInt:
            res = int(self.value + other.col)
            return SemiRingInt(res)
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        if type(other) == SemiRingInt:
            res = self.value * other.col
            return SemiRingInt(int(res))
        elif type(other) == SemiRingDictionary or type(other) == SemiRingRecord or type(other) == SemiRingSet:
            res = None
            if type(other) == SemiRingDictionary:
                res = SemiRingDictionary(
                    other.get_type()[0], other.get_type()[1])
            elif type(other) == SemiRingSet:
                res = SemiRingSet(other.get_type()[0], None)
            else:
                res = SemiRingRecord()
            for (k, v) in other.col.items():
                res[k] = self * v
            return res
        else:
            raise ValueError(str(type(self)) + " * " +
                             str(type(other)) + " is not allowed!")

    def __truediv__(self, other):
        if type(other) == SemiRingInt:
            res = self.value / other.col
            return SemiRingInt(int(res))
        else:
            raise ValueError(str(type(self)) + " / " +
                             str(type(other)) + " is not allowed!")

    def __sub__(self, other):
        if self != None and other != None and type(other) == type(self):
            return type(self)(self.value - other.col)
        else:
            raise ValueError("subtraction cannot be done.")


class SemiRingBool(SemiRing):
    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = False
        elif not isinstance(initialvalue, bool):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)

    def __add__(self, other):
        if type(other) == SemiRingBool:
            res = bool(self.value + other.col)
            return SemiRingBool(res)
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        if type(other) == SemiRingBool:
            res = self.value * other.col
            return SemiRingBool(bool(res))
        elif type(other) == SemiRingDictionary or type(other) == SemiRingRecord or type(other) == SemiRingSet:
            res = None
            if type(other) == SemiRingDictionary:
                res = SemiRingDictionary(
                    other.get_type()[0], other.get_type()[1])
            elif type(other) == SemiRingSet:
                res = SemiRingSet(other.get_type()[0], None)
            else:
                res = SemiRingRecord()
            for (k, v) in other.col.items():
                res[k] = self * v
            return res
        else:
            raise ValueError(str(type(self)) + " * " +
                             str(type(other)) + " is not allowed!")

    def __sub__(self, other):
        if self != None and other != None and type(other) == type(self):
            return type(self)(self.value - other.col)
        else:
            raise ValueError("subtraction cannot be done.")


class SemiRingEnum(SemiRing):
    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = ""
        elif not isinstance(initialvalue, str):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)


def dom(dict):
    if (type(dict) != SemiRingDictionary):
        raise ValueError("dom function can be done on dictionaries!")
    return sum(lambda pair: SemiRingDictionary(dict.get_type()[0], SemiRingBool, {pair[0]: SemiRingBool(True)}), dict)


def promote(scalar, to_type):
    value_type = type(scalar)
    if value_type == SemiRingInt and to_type == SemiRingFloat:
        return SemiRingFloat(float(scalar.col))
    elif value_type == SemiRingBool:
        if to_type == SemiRingInt:
            return SemiRingInt(int(scalar.col))
        elif to_type == SemiRingFloat:
            return SemiRingFloat(float(scalar.col))
    else:
        return scalar


def sum(func, dict):
    if dict == None or dict.col == {}:
        return SemiRingDictionary(SemiRing, SemiRing)
    res = None
    for pair in dict.col.items():
        tmp = func(pair)
        if res is None:
            if type(tmp) == SemiRingDictionary:
                res = SemiRingDictionary(tmp.get_type()[0], tmp.get_type()[1])
            elif type(tmp) == SemiRingSet:
                res = SemiRingSet(tmp.get_type()[0])
            else:
                res = type(tmp)()
        res = tmp + res
    if res == None:
        return dict
    return res


def selection(func, dict):
    def tmp_func(x):
        if func(x[0]):
            return SemiRingSet(SemiRingRecord, (x[0],))
        else:
            return SemiRingSet(SemiRingRecord)

    return sum(tmp_func, dict)


def projection(func, dict):
    return sum(lambda x: SemiRingSet(SemiRingRecord, (func(x[0]),)), dict)


def union(dict1, dict2):
    return dict1 + dict2


def intersection(dict1, dict2):
    def tmp_func(x):
        if dict2[x[0]] == SemiRingBool(True):
            return SemiRingSet(SemiRingRecord, (x[0],))
        else:
            return SemiRingSet(SemiRingRecord)

    return sum(tmp_func, dict1)


def difference(dict1, dict2):
    def tmp_func(x):
        if dict2[x[0]] == SemiRingBool(True):
            return SemiRingSet(SemiRingRecord)
        else:
            return SemiRingSet(SemiRingRecord, (x[0],))

    return sum(tmp_func, dict1)


def recordconcat(rec1, rec2, name1, name2):
    pairs = {}
    for item in rec1.col.items():
        pairs[name1 + item[0]] = item[1]
    for item in rec2.col.items():
        pairs[name2 + item[0]] = item[1]
    return SemiRingRecord(pairs)


def cartesian_product(dict1, dict2, name1, name2):
    return sum(
        lambda d1: sum(lambda d2: SemiRingSet(dict1.get_type()[0], (recordconcat(d1[0], d2[0], name1, name2),)), dict2),
        dict1)


def join(func, dict1, dict2):
    return selection(func, cartesian_product(dict1, dict2))


def hashequaljoin(dict1, dict2, tname1, tname2, colname1, colname2):
    def partition(item):
        tmp = SemiRingDictionary(SemiRingRecord, SemiRingBool, {item})
        return SemiRingDictionary(SemiRing, SemiRingDictionary, {item[0][colname1]: tmp})

    dict1_part = sum(partition, dict1)

    def f1(dic):
        pkey = dic[0][colname2]

        def f2(dic_):
            return SemiRingSet(SemiRingRecord, ((recordconcat(dic_[0], dic[0], tname1, tname2),)))

        return sum(f2, dict1_part[pkey])

    return sum(f1, dict2)


def zero(obj):
    if (type(obj) != type):
        if (type(obj) == SemiRingBool):
            return SemiRingBool(False)
        elif (type(obj) == SemiRingInt):
            return SemiRingInt(0)
        elif (type(obj) == SemiRingFloat):
            return SemiRingFloat(0.0)
        elif (type(obj) == SemiRingSet):
            return SemiRingSet(obj.get_type()[0])
        elif (type(obj) == SemiRingDictionary):
            return SemiRingDictionary(obj.get_type()[0], obj.get_type()[1])
        elif (type(obj) == SemiRingRecord):
            pairs = {}
            if len(obj.col.items()) > 0:
                for (k, v) in obj.col.items():
                    pairs[k] = zero(v)
                return SemiRingRecord(pairs)
            return SemiRingRecord()
        else:
            return None
    else:
        if (obj == SemiRingBool):
            return SemiRingBool(False)
        elif (obj == SemiRingInt):
            return SemiRingInt(0)
        elif (obj == SemiRingFloat):
            return SemiRingFloat(0.0)
        elif (obj == SemiRingSet):
            return SemiRingSet(SemiRing)
        elif (obj == SemiRingDictionary):
            return SemiRingDictionary(SemiRing, SemiRing)
        elif (obj == SemiRingRecord):
            return SemiRingRecord()
        else:
            return None


# Type Definitions #########################################


class Type:
    def __init__(self) -> None:
        pass

    def __eq__(self, other):
        if type(self) == type(other):
            return True
        else:
            return False


class IntType(Type):
    def __init__(self) -> None:
        pass


class FloatType(Type):
    def __init__(self) -> None:
        pass


class BoolType(Type):
    def __init__(self) -> None:
        pass


class StringType(Type):
    def __init__(self, charCount=None) -> None:
        self.charCount = charCount


class DictionaryType(Type):
    def __init__(self, fromType: Type = None, toType: Type = None):
        super().__init__()
        self.fromType = fromType
        self.toType = toType
        self.type_name = None

    def __eq__(self, other):
        if other is None or other == VectorType():
            return False
        if (self.fromType is None and self.toType is None) or (other.fromType is None and other.toType is None):
            return True
        if self.fromType == other.fromType and self.toType == other.toType:
            return True
        else:
            return False


class RecordType(Type):
    def __init__(self, pairTypes: List['Tuple[str,Type]'] = None):
        super().__init__()
        if pairTypes is None:
            self.typesDict = dict([])
            self.typesList = []
        self.typesDict = dict(pairTypes)
        self.typesList = pairTypes

    def __eq__(self, other):
        if self is None and other is None:
            return True
        if (self is None and other is not None) or (self is not None and other is None):
            return False
        if (type(other) != RecordType):
            return False
        if self.typesList is None or other.typesList is None:
            return True
        if len(self.typesList) != len(other.typesList):
            return False
        for (k, v) in self.typesList:
            if other.typesList[k] is None or other.typesList[k] != v:
                return False
        return True


class VectorType(Type):
    def __init__(self, exprTypes: List['Type'] = None):
        super().__init__()
        if exprTypes is None:
            self.exprTypes = []
        self.exprTypes = exprTypes

    def __eq__(self, other):
        if self is None and other is None:
            return True
        if (self is None and other is not None) or (self is not None and other is None):
            return False
        if VectorType == type(other):
            return True
        else:
            return False


class CompareSymbol(Enum):
    EQ = 1
    LT = 2
    GT = 3
    LTE = 4
    GTE = 5
    NE = 6


class ExtFuncSymbol(Enum):
    StringContains = 1
    SubStr = 2
    ToStr = 3
    ExtractYear = 4
    StartsWith = 5
    EndsWith = 6
    DictSize = 7
    FirstIndex = 8


# IR Definitions ###########################################


class Expr():
    exprList = []
    id = None
    parent = None

    def __init__(self, exprs: List['Expr']):
        self.exprList = exprs
        self.id = newExprID()

    def __add__(self, other):
        return AddExpr(self, other)

    def __sub__(self, other):
        return SubExpr(self, other)

    def __mul__(self, other):
        return MulExpr(self, other)

    def __truediv__(self, other):
        return DivExpr(self, other)

    def __floordiv__(self, other):
        return DivExpr(self, other)

    def __getattr__(self, key):
        if type(self) == PairAccessExpr or type(self) == RecConsExpr or type(self) == DicLookupExpr or type(
                self) == VarExpr:
            return RecAccessExpr(self, key)

    def __getitem__(self, key):
        if issubclass(type(key), Expr):
            return DicLookupExpr(self, key)
        elif type(key) == int:
            return PairAccessExpr(self, key)
        else:
            print("Error: get element not supported for self: " +
                  str(type(self)) + " and key:" + str(type(key)))

    def __eq__(self, other):
        return CompareExpr(CompareSymbol.EQ, self, other)

    def __ne__(self, other):
        return CompareExpr(CompareSymbol.NE, self, other)

    def __lt__(self, other):
        return CompareExpr(CompareSymbol.LT, self, other)

    def __le__(self, other):
        return CompareExpr(CompareSymbol.LTE, self, other)

    def __gt__(self, other):
        return CompareExpr(CompareSymbol.GT, self, other)

    def __ge__(self, other):
        return CompareExpr(CompareSymbol.GTE, self, other)

    def __hash__(self):
        return hash((str(self)))


class ConstantExpr(Expr):
    type = None

    def __init__(self, value):
        super().__init__(None)
        self.value = value
        if (type(value) == int):
            self.type = IntType()
        elif (type(value) == float):
            self.type = FloatType()
        elif (type(value) == bool):
            self.type = BoolType()
        elif (type(value) == str):
            self.type = StringType()
        elif value == None:
            self.type = None
        else:
            print("Error: constant type not supported!")

    def printInnerVals(self):
        return " | " + "value: " + str(self.value) + " | " + "valueType: " + str(self.type)

    def __repr__(self) -> str:
        if type(self.value) == str:
            return f'ConstantExpr("{self.value}")'
        return f'ConstantExpr({self.value})'


class VarExpr(Expr):
    def __init__(self, varName: str):
        super().__init__([])
        self.name: str = varName
        self.datasetVarName = None

    def printInnerVals(self):
        return " | " + "name: " + str(self.name)

    def __repr__(self) -> str:
        return f"{self.name}"


class LetExpr(Expr):
    def __init__(self, varExpr: VarExpr, valExpr: Expr, bodyExpr: Expr):
        super().__init__([varExpr, valExpr, bodyExpr])
        self.varExpr = varExpr
        self.valExpr = valExpr
        self.bodyExpr = bodyExpr

    def __repr__(self):
        return f'LetExpr({self.varExpr}, {self.valExpr}, {self.bodyExpr})'


class SumExpr(Expr):
    def __init__(self, varExpr: VarExpr, dictExpr: Expr, bodyExpr: Expr, isAssignmentSum=False,
                 dictType="phmap::flat_hash_map"):
        super().__init__([varExpr, dictExpr, bodyExpr])

        self.outputExpr = VarExpr(newVarName())
        self.varExpr = varExpr
        self.dictExpr = dictExpr
        self.bodyExpr = bodyExpr
        self.isAssignmentSum = isAssignmentSum
        self.dictType = dictType
        self.exprList.append(self.outputExpr)
        if self.bodyExpr == EmptyDicConsExpr:
            self.bodyExpr.parent = self

    def __repr__(self):
        return f'SumExpr({self.varExpr}, {self.dictExpr}, {self.bodyExpr}, {self.isAssignmentSum})'


class DicConsExpr(Expr):
    def __init__(self, initialPairs: List[Tuple[Expr, Expr]]):
        super().__init__([])
        self.fromType = None
        self.toType = None
        self.exprList = []
        self.initialPairs = initialPairs
        for p in initialPairs:
            self.exprList.append(p[0])
            self.exprList.append(p[1])

    def __repr__(self):
        return f'DicConsExpr({self.initialPairs})'

class EmptyDicConsExpr(Expr):
    def __init__(self):
        super().__init__(None)
        self.fromType = None
        self.toType = None
        self.exprList = []
        self.initialPairs = []

    def printInnerVals(self):
        return " | " + "fromType: " + str(self.fromType) + " | " + "toType: " + str(self.toType)


class DicLookupExpr(Expr):
    def __init__(self, dicExpr: Expr, keyExpr: Expr):
        super().__init__([dicExpr, keyExpr])
        self.dicExpr = dicExpr
        self.keyExpr = keyExpr


class RecConsExpr(Expr):
    def __init__(self, initialPairs: List[Tuple[str, Expr]]):
        super().__init__([])
        self.initialPairs = initialPairs
        for p in initialPairs:
            self.exprList.append(p[1])

    def __repr__(self):
        return f'RecConsExpr({self.initialPairs})'


class VecConsExpr(Expr):
    def __init__(self, initialExprs: List[Expr]):
        super().__init__([])
        for e in initialExprs:
            self.exprList.append(e)


class RecAccessExpr(Expr):
    def __init__(self, recExpr: Expr, fieldName: str):
        super().__init__([recExpr])
        self.name: str = fieldName
        self.recExpr = recExpr

    def printInnerVals(self):
        return " | " + "name: " + str(self.name)

    def __repr__(self) -> str:
        return f"RecAccessExpr({self.recExpr}, '{self.name}')"


class IfExpr(Expr):
    def __init__(self, condExpr: Expr, thenBodyExpr: Expr, elseBodyExpr: Expr) -> object:
        super().__init__([condExpr, thenBodyExpr, elseBodyExpr])
        self.condExpr = condExpr
        self.thenBodyExpr = thenBodyExpr
        self.elseBodyExpr = elseBodyExpr
        self.additionVarExpr = None
        self.isInNonParallelSum = False
        if self.elseBodyExpr == EmptyDicConsExpr:
            self.elseBodyExpr.parent = self

    def __repr__(self):
        return f'IfExpr({self.condExpr}, {self.thenBodyExpr}, {self.elseBodyExpr})'


class AddExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr

    def __repr__(self):
        return f'AddExpr({self.op1Expr}, {self.op2Expr})'


class SubExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr

    def __repr__(self):
        return f'SubExpr({self.op1Expr}, {self.op2Expr})'


class MulExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr

    def __repr__(self):
        return f'MulExpr({self.op1Expr}, {self.op2Expr})'


class DivExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr

    def __repr__(self):
        return f'DivExpr({self.op1Expr}, {self.op2Expr})'

class PromoteExpr(Expr):
    def __init__(self, fromType: Type, toType: Type, bodyExpr: Expr):
        super().__init__([bodyExpr])
        self.fromType = fromType
        self.toType = toType
        self.bodyExpr = bodyExpr

    def printInnerVals(self):
        return " | " + "fromType: " + str(self.fromType) + " | " + "toType: " + str(self.toType)


class CompareExpr(Expr):
    def __init__(self, compareType: CompareSymbol, leftExpr: Expr, rightExpr: Expr):
        super().__init__([leftExpr, rightExpr])
        self.compareType = compareType
        self.leftExpr = leftExpr
        self.rightExpr = rightExpr

        self.leftExpr.parent = self
        self.rightExpr.parent = self

    def printInnerVals(self):
        return " | " + "compareType: " + str(self.compareType)

    def __repr__(self):
        return f'CompareExpr({self.compareType}, {self.leftExpr}, {self.rightExpr})'


class PairAccessExpr(Expr):
    def __init__(self, pairExpr: Expr, index: int):
        super().__init__([pairExpr])
        self.index = index
        self.pairExpr = pairExpr

    def printInnerVals(self):
        return " | " + "index: " + str(self.index)

    def __repr__(self):
        return f'PairAccessExpr({self.pairExpr}, {self.index})'

class ConcatExpr(Expr):
    def __init__(self, rec1: Expr, rec2: Expr):
        super().__init__([rec1, rec2])
        self.rec1 = rec1
        self.rec2 = rec2

    def __repr__(self):
        return f'ConcatExpr({self.rec1}, {self.rec2})'


class ExtFuncExpr(Expr):
    def __init__(self, symbol: ExtFuncSymbol, inp1: Expr, inp2: Expr = None, inp3: Expr = None):
        super().__init__([inp1, inp2, inp3])
        self.symbol = symbol
        self.inp1 = inp1
        self.inp2 = inp2
        self.inp3 = inp3

    def printInnerVals(self):
        return " | " + "type: " + str(self.symbol)


# AST Helper Functions #####################################


def newExprID():
    if not hasattr(newExprID, "counter"):
        newExprID.counter = 0
    newExprID.counter += 1
    return newExprID.counter


def newVarName():
    if not hasattr(newVarName, "counter"):
        newVarName.counter = 0
    newVarName.counter += 1
    return "v" + str(newVarName.counter)


def newFuncName():
    if not hasattr(newFuncName, "counter"):
        newFuncName.counter = 0
    newFuncName.counter += 1
    return "f" + str(newFuncName.counter)


def SumBuilder(func, dictExpr: Expr, isAssignmentSum=False, dictType="phmap::flat_hash_map"):
    tmpVar = VarExpr(newVarName())
    return SumExpr(tmpVar, dictExpr, func(tmpVar), isAssignmentSum, dictType)


def LetBuilder(valExpr, bodyExprFunc):
    tmpVar = VarExpr(newVarName())
    return LetExpr(tmpVar, valExpr, bodyExprFunc(tmpVar))


def SetBuilder(exprList: List[Expr]):
    tmpList = []
    if len(exprList) > 0:
        for e in exprList:
            tmpList.append((e, ConstantExpr(True)))
        return DicConsExpr(tmpList)
    else:
        return EmptyDicConsExpr()


def JoinPartitionBuilder(dict, partitionColumn, filterFunc, outputColumns, dictType="phmap::flat_hash_map"):
    def outputCols(rec):
        tmpList = []
        if outputColumns == []:
            tmpList.append((partitionColumn, RecAccessExpr(rec, partitionColumn)))
        else:
            for col in outputColumns:
                tmpList.append((col, RecAccessExpr(rec, col)))
        return RecConsExpr(tmpList)

    def finalSelector(rec):
        if filterFunc is not None:
            return IfExpr(filterFunc(rec), DicConsExpr([(RecAccessExpr(rec, partitionColumn), outputCols(rec))]),
                          EmptyDicConsExpr())
        else:
            return DicConsExpr([(RecAccessExpr(rec, partitionColumn), outputCols(rec))])

    return SumBuilder(lambda x: finalSelector(x[0]), dict, True, dictType)


def JoinProbeBuilder(partitionedLeft, right, probeColumn, filterFunc, finalizationFunc, isAssignmentSum=False,
                     dictType="phmap::flat_hash_map"):
    probeVar = VarExpr(newVarName())

    def finalSelector(rec):
        tmpPKey = RecAccessExpr(rec, probeColumn)
        probeResult = probeVar[tmpPKey]
        if filterFunc is not None:
            return IfExpr(filterFunc(rec),
                          IfExpr(probeResult != ConstantExpr(None), finalizationFunc(probeVar[tmpPKey], rec),
                                 EmptyDicConsExpr()), EmptyDicConsExpr())
        else:
            return IfExpr(probeResult != ConstantExpr(None), finalizationFunc(probeVar[tmpPKey], rec),
                          EmptyDicConsExpr())

    return LetExpr(probeVar, partitionedLeft,
                   SumBuilder(lambda x: finalSelector(x[0]), right, isAssignmentSum, dictType))


# def SelectionBuilder(func, dictExpr: Expr):
#     return SumBuilder(lambda x: IfExpr(func(x[0]) == ConstantExpr(True), SetBuilder([x[0]]), SetBuilder([])), dictExpr)

# def ProjectionBuilder(func, dictExpr: Expr):
#     return SumBuilder(lambda x: SetBuilder([func(x[0])]), dictExpr)

# def HashJoinBuilder(S, R, colS, colR):
#     Sp = VarExpr(newVarName())
#     pkey = VarExpr(newVarName())
#     tmp = LetExpr(Sp, SumBuilder(lambda s: DicConsExpr([(RecAccessExpr(s[0], colS), DicConsExpr([s]))]), S), SumBuilder(lambda r: LetExpr(pkey, RecAccessExpr(r[0], colR),SumBuilder(lambda s: SetBuilder([ConcatExpr(r[0], s[0])]),Sp[pkey])), R))
#     return tmp

def PrintAST(expr: Expr, indentText=""):
    if (not issubclass(type(expr), Expr)):
        print("Error: Unknown AST: " + str(type(expr)))
        return

    innerText = ""
    try:
        innerText = expr.printInnerVals()
    except:
        pass

    print(indentText, end='')
    print("nodeType: " + str(type(expr)) +
          " | ID: " + str(expr.id) + innerText)

    if (expr.exprList != None):
        counter = 1
        for e in expr.exprList:
            tmpIndent = indentText.replace('─', ' ')
            tmpIndent = tmpIndent.replace('├', '│')
            tmpIndent = tmpIndent.replace('└', ' ')
            if counter < len(expr.exprList):
                tmpIndent += "├───── "
                PrintAST(e, tmpIndent)
            else:
                tmpIndent += "└───── "
                PrintAST(e, tmpIndent)
            counter += 1


def GenerateSDQLCode(AST: Expr):
    inputType = type(AST)

    if inputType == ConstantExpr:
        if type(AST.type) == StringType:
            return "\"" + AST.value + "\""
        else:
            return str(AST.value).lower()

    elif inputType == DicConsExpr:
        res = "{ "
        for e in AST.initialPairs:
            res += (GenerateSDQLCode(e[0]) + " -> " + GenerateSDQLCode(e[1]))
        return res + " }"

    elif inputType == EmptyDicConsExpr:
        fromT = "Unknown"
        toT = "Unknown"

        if type(AST.fromType) == IntType:
            fromT = "int"
        elif type(AST.fromType) == FloatType:
            fromT = "real"
        elif type(AST.fromType) == BoolType:
            fromT = "bool"
        elif type(AST.fromType) == StringType:
            fromT = "string"
        elif type(AST.fromType) == DictionaryType:
            fromT = "dictionary"
        elif type(AST.fromType) == RecordType:
            fromT = "record"

        if type(AST.toType) == IntType:
            toT = "int"
        elif type(AST.toType) == FloatType:
            toT = "real"
        elif type(AST.toType) == BoolType:
            toT = "bool"
        elif type(AST.toType) == StringType:
            toT = "string"
        elif type(AST.toType) == DictionaryType:
            toT = "dictionary"
        elif type(AST.toType) == RecordType:
            toT = "record"

        return "<" + fromT + "," + toT + ">{}"

    elif inputType == RecAccessExpr:
        return GenerateSDQLCode(AST.recExpr) + "." + AST.name

    elif inputType == IfExpr:
        return "if (" + GenerateSDQLCode(AST.condExpr) + ") then " + GenerateSDQLCode(
            AST.thenBodyExpr) + " else " + GenerateSDQLCode(AST.elseBodyExpr)

    elif inputType == AddExpr:
        return GenerateSDQLCode(AST.op1Expr) + " + " + GenerateSDQLCode(AST.op2Expr)

    elif inputType == SubExpr:
        return GenerateSDQLCode(AST.op1Expr) + " - " + GenerateSDQLCode(AST.op2Expr)

    elif inputType == MulExpr:
        return GenerateSDQLCode(AST.op1Expr) + " * " + GenerateSDQLCode(AST.op2Expr)

    elif inputType == DivExpr:
        return GenerateSDQLCode(AST.op1Expr) + " / " + GenerateSDQLCode(AST.op2Expr)

    elif inputType == DicLookupExpr:
        return GenerateSDQLCode(AST.dicExpr) + "(" + GenerateSDQLCode(AST.keyExpr) + ")"

    elif inputType == LetExpr:
        return "let " + AST.varExpr.name + " = " + GenerateSDQLCode(AST.valExpr) + " in " + GenerateSDQLCode(
            AST.bodyExpr)

    elif inputType == VarExpr:
        return AST.name

    elif inputType == PairAccessExpr:
        return GenerateSDQLCode(AST.pairExpr) + "[" + str(AST.index) + "]"

    elif inputType == PromoteExpr:

        fromT = ""
        toT = ""
        if type(AST.fromType) == IntType:
            fromT = "int"
        elif type(AST.fromType) == FloatType:
            fromT = "real"
        elif type(AST.fromType) == BoolType:
            fromT = "bool"

        if type(AST.toType) == IntType:
            toT = "int"
        elif type(AST.toType) == FloatType:
            toT = "real"
        elif type(AST.toType) == BoolType:
            toT = "bool"

        return "promote<" + fromT + "," + toT + ">(" + GenerateSDQLCode(AST.bodyExpr) + ")"

    elif inputType == CompareExpr:
        leftRes = GenerateSDQLCode(AST.leftExpr)
        rightRes = GenerateSDQLCode(AST.rightExpr)

        if AST.compareType == CompareSymbol.EQ:
            return leftRes + " == " + rightRes
        elif AST.compareType == CompareSymbol.NE:
            return leftRes + " != " + rightRes
        elif AST.compareType == CompareSymbol.LT:
            return leftRes + " < " + rightRes
        elif AST.compareType == CompareSymbol.LTE:
            return leftRes + " <= " + rightRes
        elif AST.compareType == CompareSymbol.GT:
            return leftRes + " > " + rightRes
        elif AST.compareType == CompareSymbol.GTE:
            return leftRes + " >= " + rightRes

    elif inputType == RecConsExpr:
        res = "< "
        for e in AST.initialPairs:
            res += e[0] + ": " + GenerateSDQLCode(e[1]) + ", "
        return res[:-2] + ">"

    elif inputType == VecConsExpr:
        res = "{ "
        i = 0
        for e in AST.exprList:
            res += str(i) + " -> " + GenerateSDQLCode(e) + ", "
        return res[:-2] + " }"

    elif inputType == SumExpr:
        return "sum (" + AST.varExpr.name + " in " + GenerateSDQLCode(AST.dictExpr) + ") " + GenerateSDQLCode(
            AST.bodyExpr)

    elif inputType == ConcatExpr:
        return "recordconcat(" + GenerateSDQLCode(AST.rec1) + "," + GenerateSDQLCode(AST.rec2) + ")"

    elif inputType == ExtFuncExpr:
        if AST.symbol == ExtFuncSymbol.StringContains:
            return GenerateSDQLCode(AST.inp1) + " in " + GenerateSDQLCode(AST.inp2)
        if AST.symbol == ExtFuncSymbol.SubStr:
            return GenerateSDQLCode(AST.inp1) + "[" + GenerateSDQLCode(AST.inp2) + "," + GenerateSDQLCode(
                AST.inp3) + "]"
        if AST.symbol == ExtFuncSymbol.ToStr:
            return "str(" + GenerateSDQLCode(AST.inp1) + ")"
        if AST.symbol == ExtFuncSymbol.ExtractYear:
            return "(" + GenerateSDQLCode(AST.inp1) + "/10000)"
        print("Error: Unknown ExtFuncSymbol!")
        return
    elif inputType != Expr:
        print("Error: Unknown AST: " + str(type(AST)))
        return


def interpret(AST: Expr, context: dict):
    inputType = type(AST)

    if inputType == ConstantExpr:
        if type(AST.type) == IntType:
            return SemiRingInt(AST.value)
        elif type(AST.type) == FloatType:
            return SemiRingFloat(AST.value)
        if type(AST.type) == BoolType:
            return SemiRingBool(AST.value)
        if type(AST.type) == StringType:
            return SemiRingEnum(AST.value)

    elif inputType == DicConsExpr:
        fromT = SemiRing
        toT = SemiRing

        if type(AST.fromType) == IntType:
            fromT = SemiRingInt
        elif type(AST.fromType) == FloatType:
            fromT = SemiRingFloat
        if type(AST.fromType) == BoolType:
            fromT = SemiRingBool
        if type(AST.fromType) == StringType:
            fromT = SemiRingEnum
        if type(AST.fromType) == DictionaryType:
            fromT = SemiRingDictionary
        if type(AST.fromType) == RecordType:
            fromT = SemiRingRecord

        if type(AST.toType) == IntType:
            toT = SemiRingInt
        elif type(AST.toType) == FloatType:
            toT = SemiRingFloat
        if type(AST.toType) == BoolType:
            toT = SemiRingBool
        if type(AST.toType) == StringType:
            toT = SemiRingEnum
        if type(AST.toType) == DictionaryType:
            toT = SemiRingDictionary
        if type(AST.toType) == RecordType:
            toT = SemiRingRecord

        tmpDict = {}
        for e in AST.initialPairs:
            tmpDict[interpret(e[0], context)] = interpret(e[1], context)
        return SemiRingDictionary(fromT, toT, tmpDict)

    elif inputType == EmptyDicConsExpr:
        fromT = SemiRing
        toT = SemiRing

        if type(AST.fromType) == IntType:
            fromT = SemiRingInt
        elif type(AST.fromType) == FloatType:
            fromT = SemiRingFloat
        elif type(AST.fromType) == BoolType:
            fromT = SemiRingBool
        elif type(AST.fromType) == StringType:
            fromT = SemiRingEnum
        elif type(AST.fromType) == DictionaryType:
            fromT = SemiRingDictionary
        elif type(AST.fromType) == RecordType:
            fromT = SemiRingRecord

        if type(AST.toType) == IntType:
            toT = SemiRingInt
        elif type(AST.toType) == FloatType:
            toT = SemiRingFloat
        elif type(AST.toType) == BoolType:
            toT = SemiRingBool
        elif type(AST.toType) == StringType:
            toT = SemiRingEnum
        elif type(AST.toType) == DictionaryType:
            toT = SemiRingDictionary
        elif type(AST.toType) == RecordType:
            toT = SemiRingRecord

        return SemiRingDictionary(fromT, toT)

    elif inputType == RecAccessExpr:
        return interpret(AST.recExpr, context)[AST.name]

    elif inputType == IfExpr:
        if interpret(AST.condExpr, context):
            return interpret(AST.thenBodyExpr, context)
        else:
            return interpret(AST.elseBodyExpr, context)

    elif inputType == AddExpr:
        return interpret(AST.op1Expr, context) + interpret(AST.op2Expr, context)

    elif inputType == SubExpr:
        return interpret(AST.op1Expr, context) - interpret(AST.op2Expr, context)

    elif inputType == MulExpr:
        return interpret(AST.op1Expr, context) * interpret(AST.op2Expr, context)

    elif inputType == DivExpr:
        return interpret(AST.op1Expr, context) / interpret(AST.op2Expr, context)

    elif inputType == DicLookupExpr:
        return interpret(AST.dicExpr, context)[interpret(AST.keyExpr, context)]

    elif inputType == LetExpr:
        context[AST.varExpr.name] = interpret(AST.valExpr, context)
        result = interpret(AST.bodyExpr, context)
        context.pop(AST.varExpr.name, None)
        return result

    elif inputType == VarExpr:
        return context[AST.name]

    elif inputType == PairAccessExpr:
        return interpret(AST.pairExpr, context)[AST.index]

    elif inputType == PromoteExpr:
        toT = None
        if type(AST.toType) == IntType:
            toT = SemiRingInt
        elif type(AST.toType) == FloatType:
            toT = SemiRingFloat
        elif type(AST.toType) == BoolType:
            toT = SemiRingBool
        return promote(interpret(AST.bodyExpr, context), toT)

    elif inputType == CompareExpr:
        leftRes = interpret(AST.leftExpr, context)
        rightRes = interpret(AST.rightExpr, context)
        if AST.compareType == CompareSymbol.EQ:
            return leftRes == rightRes
        elif AST.compareType == CompareSymbol.NE:
            return leftRes != rightRes
        elif AST.compareType == CompareSymbol.LT:
            return leftRes < rightRes
        elif AST.compareType == CompareSymbol.LTE:
            return leftRes <= rightRes
        elif AST.compareType == CompareSymbol.GT:
            return leftRes > rightRes
        elif AST.compareType == CompareSymbol.GTE:
            return leftRes >= rightRes

    elif inputType == RecConsExpr:
        tmpDict = {}
        for e in AST.initialPairs:
            tmpDict[e[0]] = interpret(e[1], context)
        return SemiRingRecord(tmpDict)

    elif inputType == VecConsExpr:
        tmpList = []
        for e in AST.exprList:
            tmpList.append(interpret(e, context))
        return tmpList

    elif inputType == SumExpr:
        return sum(lambda x: interpret(LetExpr(AST.varExpr, x, AST.bodyExpr), context),
                   interpret(AST.dictExpr, context))

    elif inputType == ConcatExpr:
        return recordconcat(interpret(AST.rec1, context), interpret(AST.rec2, context), "", "")

    elif inputType == ExtFuncExpr:
        if AST.symbol == ExtFuncSymbol.StringContains:
            return (interpret(AST.inp1, context).value in interpret(AST.inp2, context).value)
        if AST.symbol == ExtFuncSymbol.SubStr:
            return (interpret(AST.inp1, context).col)[
                   (interpret(AST.inp2, context).value):(interpret(AST.inp3, context).value)]
        if AST.symbol == ExtFuncSymbol.ToStr:
            return ConstantExpr(str(interpret(AST.inp1, context).value))
        if AST.symbol == ExtFuncSymbol.ExtractYear:
            return ConstantExpr(interpret(AST.inp1, context) / 10000)
        print("Error: Unknown ExtFuncSymbol: " + str(AST.symbol))
        return

    elif inputType == tuple:
        return AST

    elif inputType != Expr:
        print("Error: Unknown AST: " + str(type(AST)))
        return

############################################################
