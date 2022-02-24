from enum import Enum


class PartitionType(Enum):
    """
    Partition Type which defines the intervals for the twitter network creation
    """
    FIVE_MINUTES = '5Min'
    FIFTEEN_MINUTES = '15Min'
    ONE_HOUR = 'H'
