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
class LastFunc(Enum):
    GroupbyAgg = 0
    Agg = 1
