from enum import (
    Enum,
    unique,
)


@unique
class LogicSymbol(Enum):
    AND = 1
    OR = 2
    NOT = 3


@unique
class MathSymbol(Enum):
    ADD = 1
    SUB = 2
    MUL = 3
    DIV = 4
    MOD = 5


@unique
class AggrType(Enum):
    Scalar = 0
    Dict = 1
    Record = 2


@unique
class LastIterFunc(Enum):
    Agg = 0
    GroupbyAgg = 1
    JoinPartition = 2
    JoinProbe = 3
    Joint = 4
    Calc = 5


@unique
class OptGoal(Enum):
    UnOptimized = 0
    Infer = 1
    Aggregation = 2
    GroupBy = 3
    GroupByAggregation = 4
    JoinPartition = 5
    JoinProbe = 6
    Joint = 7


@unique
class MergeType(Enum):
    NONE = 0
    PARTITION = 1
    PROBE = 2


@unique
class SumIterType(Enum):
    Assign = 0
    Update = 1


@unique
class OpRetType(Enum):
    UNKNOWN = -1
    BOOL = 0
    INT = 1
    FLOAT = 2
    STRING = 3
    RECORD = 4
    DICT = 5

@unique
class PandasRetType(Enum):
    DATAFRAME = 1
    SERIES = 2
    SCALAR = 3
    DATETIME = 4
