from core.appmodule import AppModule
from core.logger import Logger
from core.event_bus import EventBus, Event
from cloud.cloud_client import CloudClient
from db.database import Database
from core.events import EventType
from db import model
from core import utils
import json
from enum import Enum, auto, unique
from threading import Thread, Condition, Timer
from collections import deque
import time
from collections import namedtuple


@unique
class CartOperationResult(Enum):
    OK = auto()
    NOK = auto()
    PENDING = auto()
    ERROR = auto()


@unique
class CartEventType(Enum):
    DUMMY = auto()
    PLANOGRAM_WAS_UPDATED = auto()
    PROCESS_PENDING_RESERVATIONS = auto()
    BEGIN_TRANSACTION = auto()
    TRANSACTION_COMPLETED = auto()
    RESERVATION_REQUEST_UPDATE = auto()
    RESERVATION_REQUEST_CANCEL = auto()
    RESERVATION_REQUEST_PROLONG = auto()
    RESERVATION_REQUEST_CONFIRM = auto()

# Events structure:
# PLANOGRAM_WAS_UPDATED
#   no fields
#
# PROCESS_PENDING_RESERVATIONS
#   'item': DispensingPendingItem
#
# BEGIN_TRANSACTION
#   'cart_id': int
#
# TRANSACTION_COMPLETED
#   'transaction_id': str
#   'success': bool
#
# RESERVATION_REQUEST_UPDATE
#   'transaction_id': str
#   'variant_id': int
#   'amount': int
#   'request_id': int
#
# RESERVATION_REQUEST_CANCEL
#   'transaction_id': str
#
# RESERVATION_REQUEST_PROLONG
#   'transaction_id': str
#
# RESERVATION_REQUEST_CONFIRM
#   'transaction_id': str
#   'pickup_code': str
#


class CartEvent:
    def __init__(self, ev_type: CartEventType, ev_body: dict):
        self.type = ev_type
        self.body = ev_body


ExpirationItem = namedtuple('ExpirationItem', ['obj_id', 'exp_at'])
DispensingPendingItem = namedtuple('DispensingPendingItem', ['cart_id', 'reservations'])


class CartLogic(AppModule):
    """Implements logic related to operations with virtual shopping cart both local and remote.
       Processes requests from UI and from the Online Shopping portal.
    """
    MYNAME = 'logic.cart'
    REQ_CFG_OPTIONS = ['expiration_timeout', 'prereservation_timeout', 'reservation_timeout',
                       'reservation_timeout:unit', 'reservation_timeout:value', 'order_history_timeout',
                       'order_history_timeout:unit', 'order_history_timeout:value']
    EXP_LIST_CHECK_PERIOD_SEC = 5
    EXP_TM_TICKS_IN_MINUTE = 60 // EXP_LIST_CHECK_PERIOD_SEC

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus, cloud_client: CloudClient, db: Database):
        super().__init__(CartLogic.MYNAME, config_data, logger)
        self._ev_bus = ev_bus
        self._cloud_client = cloud_client
        self._db = db
        self._event_q = deque()
        self._cv = Condition()
        self._event_thread: Thread = Thread(target=self._event_processing_worker)
        self._stopped = False
        self._expiration_seconds = 900
        self._prereservation_seconds = 1200
        self._reservation_minutes = 24*60
        self._order_history_minutes = 7*24*60
        self._reservation_exp_list: list[ExpirationItem] = list()
        self._exp_list: list[ExpirationItem] = list()
        self._order_hist_exp_list: list[ExpirationItem] = list()
        self._pending_dispensing_requests: list[DispensingPendingItem] = list()
        self._exp_timer = None
        self._exp_tm_tick_cnt = 0

    def _get_my_required_cfg_options(self) -> list:
        return CartLogic.REQ_CFG_OPTIONS

    def start(self):
        self._expiration_seconds = self._config['expiration_timeout']
        self._prereservation_seconds = self._config['prereservation_timeout']
        reservation_tm_unit = self._config['reservation_timeout']['unit']
        if reservation_tm_unit == 'M':
            self._reservation_minutes = self._config['reservation_timeout']['value']
        elif reservation_tm_unit == 'H':
            self._reservation_minutes = self._config['reservation_timeout']['value'] * 60
        else:
            self._logger.warning(f"Configured reservation timeout unit ({reservation_tm_unit}) "
                                 "is unsupported, using the default value")
        order_hist_tm_unit = self._config['order_history_timeout']['unit']
        if order_hist_tm_unit == 'M':
            self._order_history_minutes = self._config['order_history_timeout']['value']
        elif order_hist_tm_unit == 'H':
            self._order_history_minutes = self._config['order_history_timeout']['value'] * 60
        elif order_hist_tm_unit == 'D':
            self._order_history_minutes = self._config['order_history_timeout']['value'] * 24 * 60
        else:
            self._logger.warning(f"Configured order history timeout unit ({order_hist_tm_unit}) "
                                 "is unsupported, using the default value")
        iot_client = self._cloud_client.get_iot_client()
        iot_client.register_handler('transaction', self._on_transaction_updated)
        iot_client.register_handler('reservation', self._on_reservation_updated)
        self._ev_bus.subscribe(EventType.PLANOGRAM_UPDATE_DONE, self._event_handler)
        self._ev_bus.subscribe(EventType.PURCHASE_FINISHED, self._event_handler)
        self._ev_bus.subscribe(EventType.BEGIN_TRANSACTION_REQUEST, self._event_handler)
        self._on_startup()
        self._event_thread.start()
        self._exp_timer = Timer(CartLogic.EXP_LIST_CHECK_PERIOD_SEC, self._exp_list_process)
        self._logger.info("Cart Logic module started")

    def stop(self):
        self._stopped = True
        with self._cv:
            self._event_q.appendleft(CartEvent(CartEventType.DUMMY, {}))
            self._cv.notify()
        self._event_thread.join()
        self._logger.info("Cart Logic module stopped")

    def _event_processing_worker(self):
        """Processes internal events in a separate thread"""
        while not self._stopped:
            with self._cv:
                self._cv.wait_for(lambda: len(self._event_q) > 0)
                ev = self._event_q.pop()
            try:
                if ev.type == CartEventType.PLANOGRAM_WAS_UPDATED:
                    self._handle_planogram_updated()
                elif ev.type == CartEventType.PROCESS_PENDING_RESERVATIONS:
                    self._process_pending_reservations(ev.body['item'])
                elif ev.type == CartEventType.BEGIN_TRANSACTION:
                    self._begin_transaction(ev.body['cart_id'])
                elif ev.type == CartEventType.TRANSACTION_COMPLETED:
                    if ev.body['success']:
                        res, _ = self.dispense(ev.body['transaction_id'])
                    else:
                        res, _ = self.clear(ev.body['transaction_id'])
                elif ev.type == CartEventType.RESERVATION_REQUEST_UPDATE:
                    self._process_reservation_update(ev.body['transaction_id'], ev.body['variant_id'],
                                                     ev.body['amount'], ev.body['request_id'])
                elif ev.type == CartEventType.RESERVATION_REQUEST_CANCEL:
                    res, _ = self.clear(ev.body['transaction_id'])
                elif ev.type == CartEventType.RESERVATION_REQUEST_PROLONG:
                    res, _ = self.prolong(ev.body['transaction_id'])
                elif ev.type == CartEventType.RESERVATION_REQUEST_CONFIRM:
                    res, _ = self.reserve(ev.body['transaction_id'], ev.body['pickup_code'])
            except KeyError as e:
                self._logger.error(f"Failed to access some data structure - {str(e)}")

    def _event_handler(self, ev: Event):
        """Processes external events"""
        if ev.type == EventType.PLANOGRAM_UPDATE_DONE:
            with self._cv:
                self._event_q.appendleft(CartEvent(CartEventType.PLANOGRAM_WAS_UPDATED, {}))
                self._cv.notify()
        elif ev.type == EventType.PURCHASE_FINISHED:
            # Not a heavy event, process right away
            self._process_purchase_finished(ev.body)
        elif ev.type == EventType.BEGIN_TRANSACTION_REQUEST:
            with self._cv:
                self._event_q.appendleft(CartEvent(CartEventType.BEGIN_TRANSACTION, ev.body))
                self._cv.notify()

    def _on_startup(self):
        try:
            # Check if there are carts in the DB and if yes, then:
            # - for local carts in checkout state verify if they are already expired,
            #   if not, then add them to the expiration list for the remaining time
            # - for remote carts in reserved state verify if they are already expired,
            #   if not, then add them to the expiration list for the remaining time
            # - all other carts and their contents should be removed
            carts = self._db.get_carts()
            now = time.time()
            for cart in carts:
                passed_sec = now - cart.locked_at
                passed_min = passed_sec // 60
                if (cart.cart_type == model.CartType.REMOTE and cart.status == model.CartStatus.RESERVED and
                        passed_min < self._reservation_minutes):
                    remained_min = self._reservation_minutes - passed_min
                    self._reservation_exp_list.append(ExpirationItem(cart.obj_id, time.monotonic() + remained_min * 60))
                    self._logger.debug(f"Remote cart {cart.obj_id} transaction {cart.transaction_id} added "
                                       f"to expiration list for {remained_min} minutes")
                elif cart.status == model.CartStatus.CHECKOUT and passed_sec < self._expiration_seconds:
                    remained_sec = self._expiration_seconds - passed_sec
                    self._exp_list.append(ExpirationItem(cart.obj_id, time.monotonic() + remained_sec))
                    self._logger.debug(f"Local cart {cart.obj_id} display {cart.display_id} transaction "
                                       f"{cart.transaction_id} added to expiration list for {remained_sec} seconds")
                else:
                    self._db.remove_cart(cart.obj_id)
                    self._logger.debug(f"Cart {cart.obj_id} display {cart.display_id} transaction "
                                       f"{cart.transaction_id} is obsolete and cleared")
            # Check if there are pickup history records in the DB and if yes, then:
            # verify if they are already expired,
            # if not, then add them to the expiration list for the remaining time, otherwise remove them
            records = self._db.get_order_history_records()
            for rec in records:
                passed_sec = time.time() - rec.created_at
                passed_min = passed_sec // 60
                if passed_min < self._order_history_minutes:
                    remained_min = self._order_history_minutes - passed_min
                    self._order_hist_exp_list.append(ExpirationItem(rec.obj_id, time.monotonic() + remained_min * 60))
                    self._logger.debug(f"Order history record {rec.obj_id} transaction {rec.transaction_id} order "
                                       f"{rec.order_info} added to expiration list for {remained_min} minutes")
                else:
                    self._db.remove_order_history_record(rec.obj_id)
                    self._logger.debug(f"Order history record {rec.obj_id} transaction {rec.transaction_id} order "
                                       f"{rec.order_info} is obsolete and removed")
        except utils.DbError as e:
            # TODO: telemetry
            print(f"{e.funcname}:{e.msg}:{e.internal_error}")
            pass

    def _on_transaction_updated(self, msg: str):
        self._logger.debug(f"Received: ({msg})")
        try:
            data = json.loads(msg)
            transaction_id = data['transactionId']
            status = data['status']
            with self._cv:
                self._event_q.appendleft(CartEvent(CartEventType.TRANSACTION_COMPLETED,
                                                   {'transaction_id': transaction_id,
                                                    'success': True if status == 'PAYMENT_SUCCESS' else False}))
                self._cv.notify()
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to process transaction update notification - {str(e)}")
        except KeyError:
            self._logger.warning(f"Received transaction update notification is malformed")

    def _on_reservation_updated(self, msg: str):
        self._logger.debug(f"Received: ({msg})")
        try:
            data = json.loads(msg)
            transaction_id = data['transactionId']
            upd_type = data['updateType']
            if upd_type == 'update':
                with self._cv:
                    self._event_q.appendleft(CartEvent(CartEventType.RESERVATION_REQUEST_UPDATE,
                                                       {'transaction_id': transaction_id,
                                                        'variant_id': data['variantId'],
                                                        'amount': data['amount'],
                                                        'request_id': data['requestId']}))
                    self._cv.notify()
            elif upd_type == 'cancel':
                with self._cv:
                    self._event_q.appendleft(CartEvent(CartEventType.RESERVATION_REQUEST_CANCEL,
                                                       {'transaction_id': transaction_id}))
                    self._cv.notify()
            elif upd_type == 'prolong':
                with self._cv:
                    self._event_q.appendleft(CartEvent(CartEventType.RESERVATION_REQUEST_PROLONG,
                                                       {'transaction_id': transaction_id}))
                    self._cv.notify()
            elif upd_type == 'confirm':
                with self._cv:
                    self._event_q.appendleft(CartEvent(CartEventType.RESERVATION_REQUEST_CONFIRM,
                                                       {'transaction_id': transaction_id,
                                                        'pickup_code': data['pickupCode']}))
                    self._cv.notify()
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to process reservation update notification - {str(e)}")
        except KeyError:
            self._logger.warning(f"Received reservation update notification is malformed")

    def _set_prereservation_timer(self, cart_id: int, restart: bool = False):
        """Add a new prereservation expiration item to the list"""
        if restart:
            self._cancel_cart_expiration_tm(cart_id)
        self._exp_list.append(ExpirationItem(cart_id, time.monotonic() + self._prereservation_seconds))

    def _cancel_cart_expiration_tm(self, cart_id: int):
        for i in range(len(self._exp_list)):
            if self._exp_list[i].obj_id == cart_id:
                del self._exp_list[i]
                break

    def _cancel_cart_reservation_expiration_tm(self, cart_id: int):
        for i in range(len(self._reservation_exp_list)):
            if self._reservation_exp_list[i].obj_id == cart_id:
                del self._reservation_exp_list[i]
                break

    def _do_reservation(self, cart_id: int, var_id: int, amount: int) -> bool:
        """Tries to reserve amount of var_id items if this amount is available.
           Returns true in case of success and False otherwise.
        """
        quantity = 0
        inv_items = self._db.get_inventory_items_by_variant(var_id)
        for item in inv_items:
            quantity += item.quantity
        reserved = 0
        reservations = self._db.get_reservations(var_id)
        for r in reservations:
            reserved += r.quantity
        if quantity > 0 and (quantity - reserved) >= amount:
            for item in inv_items:
                already_reserved = 0
                for r in reservations:
                    if item.unit_id == r.unit_id and item.location == r.location:
                        already_reserved += r.quantity
                if (item.quantity - already_reserved) >= amount:
                    self._db.add_or_update_reservation(model.Reservation(0, cart_id, var_id, item.unit_id,
                                                                         item.location, amount))
                    break
                elif item.quantity == already_reserved:
                    continue
                else:
                    self._db.add_or_update_reservation(model.Reservation(0, cart_id, var_id, item.unit_id,
                                                                         item.location,
                                                                         item.quantity - already_reserved))
                    amount -= item.quantity - already_reserved
                    continue
            return True
        return False

    def _cancel_reservation(self, cart_id: int, var_id: int, amount: int):
        reservations = self._db.get_reservations(var_id, cart_id)
        for r in reservations:
            if r.quantity == amount:
                self._db.remove_reservation(r.id)
                break
            elif r.quantity < amount:
                self._db.remove_reservation(r.id)
                amount -= r.quantity
            else:
                r.quantity -= amount
                self._db.update_reservation(r)
                break
            if amount <= 0:
                break

    def update(self, transaction_id: str, display_id: int, cart_type: model.CartType, var_id: int,
               amount: int) -> (CartOperationResult, str):
        """Creates new cart if it wasn't yet created for the given transaction ID, changes previously reserved items
            by the given amount or creates new reservations.
        """
        try:
            self._logger.debug(f"Handling cart update for transaction {transaction_id}")
            if amount == 0:
                self._logger.warning("Requested cart update with zero amount")
                return CartOperationResult.ERROR, "Amount cannot be 0"
            is_new_cart = False
            cart = self._db.get_cart_by_transaction(transaction_id)
            if cart is None:
                cart = model.Cart(0, display_id, transaction_id, int(cart_type), '',
                                  int(model.CartStatus.CREATED) if cart_type == model.CartType.LOCAL else
                                  int(model.CartStatus.PRERESERVATION), int(model.CheckoutMethod.UNDEFINED), 0)
                cart.obj_id = self._db.add_cart(cart)
                is_new_cart = True
                if cart.status == model.CartStatus.PRERESERVATION:
                    self._set_prereservation_timer(cart.obj_id)
            cart_contents = self._db.get_cart_items(cart.obj_id)
            is_processed = False
            result = (CartOperationResult.OK, "")
            for item in cart_contents:
                if item.variant_id == var_id:
                    if amount > 0:
                        if self._do_reservation(cart.obj_id, var_id, amount):
                            item.amount += amount
                            self._db.update_cart_item(item)
                            self._logger.debug(f"Increased number of items in the cart of {var_id} by {amount}")
                        else:
                            self._logger.warning("Failed to increase number of items in the cart of "
                                                 f"{var_id} by {amount}")
                            result = (CartOperationResult.NOK, "")
                    else:
                        abs_amount = abs(amount)
                        if item.amount >= abs_amount:
                            self._cancel_reservation(cart.obj_id, var_id, abs_amount)
                            if (item.amount - abs_amount) > 0:
                                item.amount -= abs_amount
                                self._db.update_cart_item(item)
                            else:
                                self._db.remove_cart_item(item)
                            self._logger.debug(f"Decreased number of items in the cart of {var_id} by {abs_amount}")
                        else:
                            self._logger.warning(f"Requested to remove from cart {cart.obj_id} more items of "
                                                 f"{var_id} than it contains")
                            result = (CartOperationResult.ERROR, f"Requested amount {abs_amount} is more than reserved")
                    is_processed = True
            if not is_processed:
                if amount > 0:
                    if self._do_reservation(cart.obj_id, var_id, amount):
                        item = model.CartItem(cart.obj_id, var_id, amount)
                        self._db.add_cart_item(item)
                        self._logger.debug(f"Added {amount} of {var_id} to the cart")
                    else:
                        self._logger.warning(f"Failed to add {amount} of {var_id} to the cart")
                        result = (CartOperationResult.NOK, "")
                else:
                    self._logger.warning(f"Requested to remove from cart {cart.obj_id} not yet added items of {var_id}")
                    result = (CartOperationResult.ERROR, "Cannot remove not yet added items")
            if (not is_new_cart and cart.status == model.CartStatus.PRERESERVATION
                    and result[0] == CartOperationResult.OK):
                self._set_prereservation_timer(cart.obj_id, restart=True)
            return result
        except utils.DbError as e:
            # TODO: telemetry
            return CartOperationResult.ERROR, "Internal error"

    def clear(self, transaction_id: str) -> (CartOperationResult, str):
        """Clears cart, its contents and connected reservations. Aborts all expiration timeouts."""
        try:
            self._logger.debug(f"Handling cart clear for transaction {transaction_id}")
            cart = self._db.get_cart_by_transaction(transaction_id)
            if cart is None:
                self._logger.warning(f"Trying to clear cart for transaction {transaction_id} but it does not exist")
                return CartOperationResult.ERROR, "Cart is not found"
            self._cancel_cart_expiration_tm(cart.obj_id)
            if cart.cart_type == model.CartType.REMOTE:
                self._cancel_cart_reservation_expiration_tm(cart.obj_id)
            self._db.remove_cart(cart.obj_id)
            return CartOperationResult.OK, ""
        except utils.DbError as e:
            # TODO: telemetry
            return CartOperationResult.ERROR, "Internal error"

    def prolong(self, transaction_id: str) -> (CartOperationResult, str):
        """Used by the Online Shopping portal to prolong prereservation of a remote cart"""
        try:
            self._logger.debug(f"Handling cart prolong for transaction {transaction_id}")
            cart = self._db.get_cart_by_transaction(transaction_id)
            if cart is None:
                self._logger.warning(f"Trying to prolong cart for transaction {transaction_id} but it does not exist")
                return CartOperationResult.ERROR, "Cart is not found"
            if cart.cart_type == model.CartType.REMOTE and cart.status == model.CartStatus.PRERESERVATION:
                self._set_prereservation_timer(cart.obj_id, restart=True)
            else:
                self._logger.warning(f"Trying to prolong cart {cart.obj_id} for transaction {transaction_id}, "
                                     "but either it is not remote or its state is incorrect")
                return CartOperationResult.ERROR, "Wrong cart type or state to prolong"
            return CartOperationResult.OK, ""
        except utils.DbError as e:
            # TODO: telemetry
            return CartOperationResult.ERROR, "Internal error"

    def reserve(self, transaction_id: str, order_info: str) -> (CartOperationResult, str):
        """Used by the Online Shopping portal to reserve a cart for subsequent pick up"""
        try:
            self._logger.debug(f"Handling cart reserve for transaction {transaction_id}")
            cart = self._db.get_cart_by_transaction(transaction_id)
            if cart is None:
                self._logger.info(f"Trying to reserve cart for transaction {transaction_id} but it does not exist")
                return CartOperationResult.ERROR, "Cart is not found"
            if cart.cart_type == model.CartType.REMOTE:
                self._cancel_cart_expiration_tm(cart.obj_id)
                cart.order_info = order_info
                cart.checkout_method = model.CheckoutMethod.PICKUP
                cart.status = model.CartStatus.RESERVED
                cart.locked_at = time.time()
                self._db.update_cart(cart)
                self._reservation_exp_list.append(ExpirationItem(cart.obj_id,
                                                                 time.monotonic() + self._reservation_minutes * 60))
            else:
                self._logger.warning(f"Trying to reserve cart {cart.obj_id} for transaction {transaction_id}, "
                                     "but it is not remote")
                return CartOperationResult.ERROR, "Wrong cart type to reserve"
            return CartOperationResult.OK, ""
        except utils.DbError as e:
            # TODO: telemetry
            return CartOperationResult.ERROR, "Internal error"

    def dispense(self, transaction_id: str, display_id: int = 0) -> (CartOperationResult, str):
        """Initiate dispensing process for the cart with the given transaction ID"""
        try:
            self._logger.debug(f"Handling cart dispense for transaction {transaction_id}")
            cart = self._db.get_cart_by_transaction(transaction_id)
            if cart is None:
                self._logger.warning(f"Trying to start dispensing of cart for transaction {transaction_id} "
                                     "but it does not exist")
                return CartOperationResult.ERROR, "Cart is not found"
            cart_contents = self._db.get_cart_items(cart.obj_id)
            if len(cart_contents) == 0:
                self._logger.warning(f"Trying to start dispensing for cart {cart.obj_id} transaction {transaction_id} "
                                     "but it is empty")
                return CartOperationResult.ERROR, "Cart is empty"
            self._cancel_cart_expiration_tm(cart.obj_id)
            if cart.cart_type == model.CartType.REMOTE:
                self._cancel_cart_reservation_expiration_tm(cart.obj_id)
                # Set display_id in cart to be used to show dispensing progress
                cart.display_id = display_id
                self._db.update_cart(cart)
            reservations = list()
            for item in cart_contents:
                var_reservations = self._db.get_reservations(item.variant_id, cart.obj_id)
                reservations = reservations + var_reservations
            # TODO: Call dispensing logic module to start dispensing of reservations
            # if start dispensing
            #    cart.status = model.CartStatus.DISPENSING
            #    self._db.update_cart(cart)
            # else:
            #    self._logger.into(f"Cannot start dispensing for cart {cart.obj_id} transaction {transaction_id} "
            #                      f"order {cart.order_info}, put to the queue")
            #    self._pending_dispensing_requests.append(DispensingPendingItem(cart.obj_id, reservations))
            #    return CartOperationResult.PENDING, ""
            return CartOperationResult.OK, ""
        except utils.DbError as e:
            # TODO: telemetry
            return CartOperationResult.ERROR, "Internal error"

    def _handle_planogram_updated(self):
        """Check if there are reserved variants and if their locations were changed due to planogram update.
           If yes, then update reservations accordingly.
        """
        var_locations: dict[dict[list]] = dict()
        try:
            carts = self._db.get_carts()
            for cart in carts:
                cart_contents = self._db.get_cart_items(cart.obj_id)
                for item in cart_contents:
                    var_id = item.variant_id
                    inv_items = self._db.get_inventory_items_by_variant(var_id)
                    if var_id not in var_locations:
                        var_locations[var_id] = dict()
                        for inv_item in inv_items:
                            if inv_item.unit_id in var_locations[var_id]:
                                var_locations[var_id][inv_item.unit_id].append(inv_item.location)
                            else:
                                var_locations[var_id][inv_item.unit_id] = list()
                                var_locations[var_id][inv_item.unit_id].append(inv_item.location)
                    reservations = self._db.get_reservations(var_id, cart.obj_id)
                    used_locations = list()
                    # First pass, lookup for not changed locations
                    for r in reservations:
                        if r.unit_id not in var_locations[var_id]:
                            # Should not happen
                            self._logger.critical("Reservations and Inventory are out of sync. Reserved item of "
                                                  f"{var_id} is expected to be in unit {r.unit_id}, but not found")
                            # TODO: telemetry
                        else:
                            if r.location in var_locations[var_id][r.unit_id]:
                                # Assume that reservations for one cart are all in the same unit.
                                # If a location was not changed that it cannot be a target for other, changed locations.
                                used_locations.append(r.location)
                    # Second pass, update changed locations
                    for r in reservations:
                        if r.location not in var_locations[var_id][r.unit_id]:
                            # Variant was moved from this location, need to find another one
                            updated = False
                            for loc in var_locations[var_id][r.unit_id]:
                                if loc not in used_locations:
                                    # Assume that after planogram update all variants used in reservations
                                    # occupy not less slots within one unit than before update.
                                    # So, once a new location is assigned to a reservation,
                                    # it cannot be assigned more (within the cart).
                                    # Also assume that after update enough amount of products are in new slots
                                    # to let the reserved items be dispensed later on. We cannot control it in SW
                                    self._db.update_reservation(r, loc)
                                    used_locations.append(loc)
                                    updated = True
                                    self._logger.debug(f"Reserved variant {var_id} in cart {cart.obj_id} transaction "
                                                       f"{cart.transaction_id} in unit {r.unit_id} changed location "
                                                       f"from {r.location} to {loc}")
                                    break
                            if not updated:
                                self._logger.critical(f"Failed to relocate reserved variant {var_id} in unit "
                                                      f"{r.unit_id} location {r.location}")
                                # TODO: telemetry
        except KeyError as e:
            self._logger.error(f"Data structure error occurred while trying to relocate reserved items - {str(e)}")
            # TODO: telemetry
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _exp_list_process(self):
        """Walk through the expiration lists, check if an item is expired and process it"""
        try:
            # First part, short timers
            items_to_erase = list()
            for exp_item in self._exp_list:
                if time.monotonic() > exp_item.exp_at:
                    cart = self._db.get_cart(exp_item.obj_id)
                    if cart is None:
                        self._logger.warning(f"Cart {exp_item.obj_id} is expired but failed to find it in DB")
                    else:
                        if cart.status == model.CartStatus.PRERESERVATION:
                            self._ev_bus.post(Event(EventType.RESERVATION_COMPLETED,
                                                    {'transaction_id': cart.transaction_id,
                                                     'status': model.ReservationCompletionStatus.EXPIRED}))
                        self._db.remove_cart(cart.obj_id)
                        self._logger.debug(f"Cart {cart.obj_id} display {cart.display_id} transaction "
                                           f"{cart.transaction_id} is expired and cleared")
                    items_to_erase.append(exp_item)
            for item in items_to_erase:
                self._exp_list.remove(item)
            # Second part, long timers
            self._exp_tm_tick_cnt += 1
            if self._exp_tm_tick_cnt >= CartLogic.EXP_TM_TICKS_IN_MINUTE:
                self._exp_tm_tick_cnt = 0
                items_to_erase.clear()
                for exp_item in self._reservation_exp_list:
                    if time.monotonic() > exp_item.exp_at:
                        cart = self._db.get_cart(exp_item.obj_id)
                        if cart is None:
                            self._logger.warning(f"Remote cart {exp_item.obj_id} is expired "
                                                 "but failed to find it in DB")
                        else:
                            self._ev_bus.post(Event(EventType.RESERVATION_COMPLETED,
                                                    {'transaction_id': cart.transaction_id,
                                                     'status': model.ReservationCompletionStatus.EXPIRED}))
                            order_hist_rec = model.OrderHistoryRecord(0, cart.transaction_id, cart.order_info,
                                                                      model.ReservationCompletionStatus.EXPIRED,
                                                                      time.time())
                            order_hist_rec.obj_id = self._db.add_order_history_record(order_hist_rec)
                            self._order_hist_exp_list.append(ExpirationItem(order_hist_rec.obj_id, time.monotonic() +
                                                                            self._order_history_minutes * 60))
                            self._db.remove_cart(cart.obj_id)
                            self._logger.debug(f"Remote cart {cart.obj_id} transaction {cart.transaction_id}"
                                               " is expired and cleared")
                        items_to_erase.append(exp_item)
                for item in items_to_erase:
                    self._reservation_exp_list.remove(item)
                items_to_erase.clear()
                for exp_item in self._order_hist_exp_list:
                    if time.monotonic() > exp_item.exp_at:
                        self._db.remove_order_history_record(exp_item.obj_id)
                        self._logger.debug(f"Order history record {exp_item.obj_id} is expired and cleared")
                        items_to_erase.append(exp_item)
                for item in items_to_erase:
                    self._order_hist_exp_list.remove(item)
        except utils.DbError as e:
            # TODO: telemetry
            pass
        finally:
            self._exp_timer = Timer(CartLogic.EXP_LIST_CHECK_PERIOD_SEC, self._exp_list_process)

    def _process_purchase_finished(self, params: dict):
        try:
            self._logger.debug(f"Process purchase complete event for cart {params['cart_id']}")
            cart = self._db.get_cart(params['cart_id'])
            if cart is None:
                self._logger.warning(f"Purchase is complete but failed to find the cart for id {params['cart_id']}")
            else:
                if cart.cart_type == model.CartType.REMOTE:
                    self._ev_bus.post(Event(EventType.RESERVATION_COMPLETED,
                                            model.ReservationCompletionStatus.DISPENSED))
                    order_hist_rec = model.OrderHistoryRecord(0, cart.transaction_id, cart.order_info,
                                                              model.ReservationCompletionStatus.DISPENSED,
                                                              time.time())
                    order_hist_rec.obj_id = self._db.add_order_history_record(order_hist_rec)
                    self._order_hist_exp_list.append(ExpirationItem(order_hist_rec.obj_id, time.monotonic() +
                                                                    self._order_history_minutes * 60))
                self._db.remove_cart(cart.obj_id)
            # Check if there are pending dispensing requests and generate an event to process the first one
            if len(self._pending_dispensing_requests) > 0:
                pending_item = self._pending_dispensing_requests.pop(0)
                with self._cv:
                    self._event_q.appendleft(CartEvent(CartEventType.PROCESS_PENDING_RESERVATIONS,
                                                       {'item': pending_item}))
                    self._cv.notify()
        except KeyError as e:
            self._logger.error(f"Failed to access data structures - {str(e)}")
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _process_pending_reservations(self, pending_itme: DispensingPendingItem):
        """Try to initiate dispensing again for the pending reservations"""
        try:
            self._logger.debug(f"Process pending reservations for cart {pending_itme.cart_id}")
            cart = self._db.get_cart(pending_itme.cart_id)
            if cart is None:
                # Should not happen
                self._logger.error(f"Trying to start dispensing of pending cart {pending_itme.cart_id} "
                                   "but it does not exist")
                return
            # TODO: Call dispensing logic module to start dispensing of reservations
            # if start dispensing
            #    cart.status = model.CartStatus.DISPENSING
            #    self._db.update_cart(cart)
            # else:
            # Maybe good to have a mechanism to limit number of attempts to dispense same reservations
            #    self._logger.into(f"Cannot start dispensing for pending cart {cart.obj_id} transaction "
            #                      f"{transaction_id} order {cart.order_info}, put to the queue again")
            #    self._pending_dispensing_requests.append(pending_item)
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _begin_transaction(self, cart_id: int):
        """Pushes the cart's contents to the Cloud and requests to initiate transaction,
           expecting to get transaction ID back. Broadcasts the got transaction ID using event bus.
        """
        is_ok = False
        try:
            cart = self._db.get_cart(cart_id)
            if cart is None:
                # Should not happen
                self._logger.error(f"Trying to begin transaction for cart {cart_id} but failed to find the cart")
                self._ev_bus.post(Event(EventType.BEGIN_TRANSACTION_RESPONSE, {'cart_id': cart_id, 'success': False}))
                return
            cart_contents = self._db.get_cart_items(cart_id)
            if len(cart_contents) == 0:
                # Should not happen
                self._logger.error(f"Trying to begin transaction for cart {cart_id} but it is empty")
                self._ev_bus.post(Event(EventType.BEGIN_TRANSACTION_RESPONSE, {'cart_id': cart_id, 'success': False}))
                return
            req = {'deviceId': '', 'products': []}
            for item in cart_contents:
                req['products'].append({'id': item.variant_id, 'qty': item.amount})
            res = self._cloud_client.invoke_api_post_with_response('transaction', req)
            cart.transaction_id = res['transactionId']
            cart.status = model.CartStatus.CHECKOUT
            cart.locked_at = time.time()
            self._db.update_cart(cart)
            self._exp_list.append(ExpirationItem(cart_id, time.monotonic() + self._expiration_seconds))
            is_ok = True
        except utils.CloudApiNotFound:
            self._logger.error("{POST API for transaction is not found in the Cloud client")
        except utils.CloudApiFormatError as e:
            self._logger.error(f"POST API for transaction returned malformed response - {e.msg}")
        except utils.CloudApiServerError as e:
            self._logger.error(f"Failed to post transaction data to the Cloud, server returned: code {e.status_code}, "
                               f"message ({e.response})")
        except utils.CloudApiConnectionError as e:
            self._logger.error(f"Failed to connect to the Cloud to post transaction data - {e.msg}")
        except utils.CloudApiTimeoutError:
            self._logger.error("Failed to post transaction data to the Cloud due to timeout")
        except KeyError as e:
            self._logger.error(f"Received initiate transaction response is malformed - {str(e)}")
        except utils.DbError as e:
            # TODO: telemetry
            pass
        finally:
            if not is_ok:
                self._ev_bus.post(Event(EventType.BEGIN_TRANSACTION_RESPONSE, {'cart_id': cart_id, 'success': False}))

    def _process_reservation_update(self, transaction_id: str, variant_id: int, amount: int, request_id: int):
        """Processes reservation request from the Online Shopping portal.
           Tries to update the remote cart and sends the result back to the cloud using the corresponding API
        """
        res, _ = self.update(transaction_id, 0, model.CartType.REMOTE, variant_id, amount)
        try:
            resp = {'deviceId': '', 'transactionId': transaction_id,
                    'requestId': request_id, 'result': True if res == CartOperationResult.OK else False}
            self._cloud_client.invoke_api_post('prereservation', resp)
        except utils.CloudApiNotFound:
            self._logger.error("{POST API for prereservation is not found in the Cloud client")
        except utils.CloudApiServerError as e:
            self._logger.error(f"Failed to post preservation response data to the Cloud, server returned: code "
                               f"{e.status_code}, message ({e.response})")
        except utils.CloudApiConnectionError as e:
            self._logger.error(f"Failed to connect to the Cloud to post prereservation response data - {e.msg}")
        except utils.CloudApiTimeoutError:
            self._logger.error("Failed to post prereservation response data to the Cloud due to timeout")