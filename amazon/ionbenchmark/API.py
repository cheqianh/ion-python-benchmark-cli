from enum import Enum


# Serialization/deserialization APIs to benchmark.
class API(Enum):
    """Enumeration of the APIs."""
    SIMPLE_ION = 'simpleIon'
    ITERATOR = 'iterator'
    NON_BLOCKING = 'nonBlocking'
