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
    VAL = 0
    DICT = 1
    REC = 2


@unique
class LastIterFunc(Enum):
    Agg = 0
    GroupbyAgg = 1
    JoinPartition = 2
    JoinProbe = 3
    Joint = 4
    Calculation = 5


@unique
class OptGoal(Enum):
    UnOptimized = 0
    Aggregation = 1
    GroupBy = 2
    GroupByAggregation = 3
    JoinPartition = 4
    JoinProbe = 5
    Joint = 6


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
class OperationReturnType(Enum):
    BOOL = 0
    INT = 1
    FLOAT = 2
    STRING = 3
    RECORD = 4
    DICT = 5
