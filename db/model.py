from collections import namedtuple
from enum import Enum, auto, unique


@unique
class AccessLevel(Enum):
    ADMIN = auto()


User = namedtuple('User', ['name', 'password', 'access_level', 'last_logged_in'])
