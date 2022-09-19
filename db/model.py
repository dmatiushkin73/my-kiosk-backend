from collections import namedtuple
from enum import Enum, auto, unique, IntEnum

MAX_UNITS = 1


@unique
class AccessLevel(Enum):
    ADMIN = auto()


@unique
class CartStatus(IntEnum):
    CREATED = 0
    PRERESERVATION = 1
    RESERVED = 2
    CHECKOUT = 3
    DISPENSING = 4
    COMPLETE = 5


@unique
class CheckoutMethod(IntEnum):
    UNDEFINED = 0
    MOBILE = 1
    LOCAL = 2
    PICKUP = 3


@unique
class CartType(IntEnum):
    UNDEFINED = 0
    LOCAL = 1
    REMOTE = 2


@unique
class ReservationCompletionStatus(IntEnum):
    EXPIRED = 1
    DISPENSED = 2


User = namedtuple('User', ['name', 'password', 'access_level', 'last_logged_in'])
ObjectProperty = namedtuple('Property', ['ptype', 'name', 'value'])
ObjectInfo = namedtuple('Info', ['name', 'description'])
Media = namedtuple('Media', ['filename', 'last_update'])
VariantOption = namedtuple('VariantOption', ['variant_id', 'option', 'value'])
CartItem = namedtuple('CartItem', ['cart_id', 'variant_id', 'amount'])
Reservation = namedtuple('Reservation', ['id', 'cart_id', 'variant_id', 'unit_id', 'location', 'quantity'])
InventoryItem = namedtuple('InventoryItem', ['unit_id', 'tray_number', 'location', 'variant_id', 'width', 'quantity',
                                             'depth'])


class Collection:
    def __init__(self, obj_id: int, last_update: float, media_id: int | None):
        self.obj_id = obj_id
        self.last_update = last_update
        self.media_id = media_id
        self.info = dict(ObjectInfo)
        self.media: Media = None
        self.products = list()

    def add_info(self, lang: str, obj_info: ObjectInfo):
        self.info[lang] = obj_info

    def set_media(self, m: Media):
        self.media = m

    def add_product(self, prod_id: int):
        self.products.append(prod_id)

    def clear_info(self):
        self.info.clear()

    def clear_products(self):
        self.products.clear()


class Product:
    def __init__(self, obj_id: int, last_update: float, prod_type: str, tags: str):
        self.obj_id = obj_id
        self.last_update = last_update
        self.prod_type = prod_type
        self.tags = tags
        self.info = dict(ObjectInfo)
        self.props = dict(ObjectProperty)
        self.variants = list()

    def add_info(self, lang: str, obj_info: ObjectInfo):
        self.info[lang] = obj_info

    def add_prop(self, lang: str, obj_prop: ObjectProperty):
        self.props[lang] = obj_prop

    def add_variant(self, variant_id: int):
        self.variants.append(variant_id)

    def clear_info(self):
        self.info.clear()

    def clear_props(self):
        self.props.clear()


class Variant:
    def __init__(self, obj_id:int, prod_id: int, price: int, price_comp: int, price_fmt: str, price_comp_fmt: str,
                 deleted: int, media_id: int | None):
        self.obj_id = obj_id
        self.prod_id = prod_id
        self.price = price
        self.price_comp = price_comp
        self.price_fmt = price_fmt
        self.price_comp_fmt = price_comp_fmt
        self.deleted = False if deleted == 0 else True
        self.media_id = media_id
        self.info = dict(ObjectInfo)
        self.options = list(VariantOption)
        self.props = dict(ObjectProperty)
        self.media: Media = None

    def set_media(self, m: Media):
        self.media = m

    def add_info(self, lang: str, obj_info: ObjectInfo):
        self.info[lang] = obj_info

    def add_prop(self, lang: str, obj_prop: ObjectProperty):
        self.props[lang] = obj_prop

    def add_option(self, opt: VariantOption):
        self.options.append(opt)

    def clear_info(self):
        self.info.clear()

    def clear_props(self):
        self.props.clear()

    def clear_options(self):
        self.options.clear()


class Cart:
    def __init__(self, obj_id: int, display_id: int, transaction_id: str, cart_type: int, order_info: str,
                 status: int, checkout_method: int, locked_at: float):
        self.obj_id = obj_id
        self.display_id = display_id
        self.transaction_id = transaction_id
        self.cart_type = CartType(cart_type)
        self.order_info = order_info
        self.status = CartStatus(status)
        self.checkout_method = CheckoutMethod(checkout_method)
        self.locked_at = locked_at


class OrderHistoryRecord:
    def __init__(self, obj_id: int, transaction_id: str, order_info: str, completion_status: int, created_at: float):
        self.obj_id = obj_id
        self.transaction_id = transaction_id
        self.order_info = order_info
        self.completion_status = ReservationCompletionStatus(completion_status)
        self.created_at = created_at
