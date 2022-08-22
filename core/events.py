from enum import Enum, auto, unique


@unique
class EventType(Enum):
    DUMMY = auto()
    SEND_TO_CLOUD = auto()

# Events structure:
# SEND_TO_CLOUD:
#   'api':  str      name of cloud API,
#   'data': dict     object to post
