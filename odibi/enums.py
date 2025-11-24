from enum import Enum


class EngineType(str, Enum):
    PANDAS = "pandas"
    SPARK = "spark"
