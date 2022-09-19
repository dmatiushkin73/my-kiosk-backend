from enum import Enum, auto, unique, IntEnum


@unique
class PlanogramStatusReason(IntEnum):
    NO_REASON = 0
    RESERVED_PRODUCT_ABSENT = 1
    RESERVED_PRODUCT_OCCUPIES_LESS_SLOTS = 2


@unique
class EventType(Enum):
    DUMMY = auto()
    SEND_TO_CLOUD = auto()
    BRAND_INFO_UPDATED = auto()
    UI_MODEL_UPDATED = auto()
    NEW_PLANOGRAM_AVAILABLE = auto()
    NEW_PLANOGRAM_APPLY = auto()
    NEW_PLANOGRAM_REJECT = auto()
    PLANOGRAM_UPDATE_DONE = auto()
    GET_PLANOGRAM = auto()
    PLANOGRAM_IS_UP_TO_DATE = auto()
    PLANOGRAM_UPDATE_FAILED = auto()
    RESERVATION_COMPLETED = auto()
    PURCHASE_FINISHED = auto()
    BEGIN_TRANSACTION_REQUEST = auto()
    BEGIN_TRANSACTION_RESPONSE = auto()


# Events structure:
# SEND_TO_CLOUD:
#   'api':  str      name of cloud API,
#   'data': dict     object to post
#
# BRAND_INFO_UPDATED:
#   no fields
#
# UI_MODEL_UPDATED:
#   no fields
#
# NEW_PLANOGRAM_AVAILABLE
#   'status': bool
#   'reason': model.PlanogramStatusReason
#
# NEW_PLANOGRAM_APPLY
#   no fields
#
# NEW_PLANOGRAM_REJECT
#   no fields
#
# PLANOGRAM_UPDATE_DONE
#   no fields
#
# GET_PLANOGRAM
#   no fields
#
# PLANOGRAM_IS_UP_TO_DATE
#   no fields
#
# PLANOGRAM_UPDATE_FAILED
#   no fields
#
# RESERVATION_COMPLETED
#   'transaction_id': str
#   'status': model.ReservationCompletionStatus
#
# PURCHASE_FINISHED
#   'cart_id': int
#
# BEGIN_TRANSACTION_REQUEST
#   'cart_id': int
#
# BEGIN_TRANSACTION_RESPONSE
#   'cart_id': int
#   'success': bool
#
