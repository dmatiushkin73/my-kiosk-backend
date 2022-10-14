from http import HTTPStatus
import json
from pathlib import Path
from collections import namedtuple
from threading import Event as AtomicFlag
from core.rest_server import RestServerBase, RequestHandlerResult
from core.logger import Logger
from core.event_bus import EventBus, Event
from core.events import EventType
from db.database import Database
from logic.cart import CartLogic, CartOperationResult
from core import utils
from db import model

RequestHandlerDesc = namedtuple('RequestHandlerDesc', ['handler', 'num_of_expected_params', 'disp_id_req'])
TransactionIdRequestData = namedtuple('TransactionIdRequestData', ['cart_id', 'waiting', 'success'])


class BackendRestServer(RestServerBase):
    """Provides REST API for the Kiosk UI"""
    MYNAME = 'ui.rest'
    REQ_CFG_OPTIONS = ['port', 'ui_model_filename', 'brand_info_filename', 'media', 'transaction_id_timeout']

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus, db: Database, cart_logic: CartLogic,
                 data_dir: Path, lang: str):
        super().__init__(BackendRestServer.MYNAME, config_data, logger)
        self._ev_bus = ev_bus
        self._db = db
        self._data_dir = data_dir
        self._lang = lang
        self._cart_logic = cart_logic
        self._get_handlers: dict[str, RequestHandlerDesc] = dict()
        self._post_handlers: dict[str, RequestHandlerDesc] = dict()
        self._put_handlers: dict[str, RequestHandlerDesc] = dict()
        self._delete_handlers: dict[str, RequestHandlerDesc] = dict()
        self._tr_id_ready_flags: list[AtomicFlag] = list()
        self._tr_id_request_data: list[TransactionIdRequestData] = list()

    def start(self):
        super().start()
        for _ in range(model.MAX_DISPLAYS):
            self._tr_id_ready_flags.append(AtomicFlag())
            self._tr_id_request_data.append(TransactionIdRequestData(0, False, False))
        self._get_handlers['test'] = RequestHandlerDesc(self._process_get_test, 0, False)
        self._get_handlers['brand-info'] = RequestHandlerDesc(self._process_get_brand_info, 0, False)
        self._get_handlers['ui-model'] = RequestHandlerDesc(self._process_get_ui_model, 0, False)
        self._get_handlers['collections'] = RequestHandlerDesc(self._process_get_collection, 1, False)
        self._get_handlers['products'] = RequestHandlerDesc(self._process_get_product, 1, False)
        self._put_handlers['cart'] = RequestHandlerDesc(self._process_put_cart, 0, True)
        self._delete_handlers['cart'] = RequestHandlerDesc(self._process_delete_cart, 0, True)
        self._post_handlers['pickup'] = RequestHandlerDesc(self._process_post_pickup, 0, True)
        self._ev_bus.subscribe(EventType.BEGIN_TRANSACTION_RESPONSE, self._app_event_handler)

    def _get_my_required_cfg_options(self) -> list:
        return BackendRestServer.REQ_CFG_OPTIONS

    @staticmethod
    def _get_my_transaction_id(display_id: int) -> str:
        return "unassigned#" + str(display_id)

    def _app_event_handler(self, ev: Event):
        """Processes external events"""
        try:
            if ev.type == EventType.BEGIN_TRANSACTION_RESPONSE:
                for display_id in range(1, model.MAX_DISPLAYS + 1):
                    if (self._tr_id_request_data[display_id - 1].waiting
                            and self._tr_id_request_data[display_id - 1].cart_id == ev.body['cart_id']):
                        self._tr_id_request_data[display_id - 1].success = ev.body['success']
                        self._tr_id_ready_flags[display_id - 1].set()
                        break
        except KeyError as e:
            self._logger.error(f"Failed to process event {ev.type} due to data access error - {str(e)}")

    def _parse_display_id(self, headers: dict) -> int:
        if 'displayId' in headers:
            try:
                display_id = int(headers['displayId'])
                return display_id
            except ValueError:
                self._logger.warning("Failed to get display_id from the request's headers")
                return model.NONEXISTENT_DISPLAY_ID
        self._logger.warning("Unable to locate 'displayId' header in the request")
        return model.NONEXISTENT_DISPLAY_ID

    def _on_get(self, path: str, headers: dict, msg: str) -> RequestHandlerResult:
        self._logger.debug(f"Received GET request for {path}")
        try:
            parts = path.split('/')
            if parts[1] not in self._get_handlers:
                self._logger.warning(f"Endpoint {parts[1]} not found")
                return self._generate_reply(HTTPStatus.NOT_FOUND, resp_txt=f"Endpoint {parts[1]} does not exist",
                                            content_type='text')
            handler_desc = self._get_handlers[parts[1]]
            if (len(parts) - 2) < handler_desc.num_of_expected_params:
                self._logger.warning(f"For endpoint {parts[1]} expected {handler_desc.num_of_expected_params} "
                                     f"parameters but found {len(parts) - 2}")
                return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Mandatory parameter(s) required",
                                            content_type='text')
            display_id = model.NONEXISTENT_DISPLAY_ID
            if handler_desc.disp_id_req:
                display_id = self._parse_display_id(headers)
                if display_id == model.NONEXISTENT_DISPLAY_ID:
                    return self._generate_reply(HTTPStatus.BAD_REQUEST,
                                                resp_txt="Mandatory header is absent or incorrect",
                                                content_type='text')
            return handler_desc.handler(parts[2:], display_id)
        except IndexError as e:
            self._logger.error(f"Failed to get endpoint or query parameter from the path - {str(e)}")
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _on_post(self, path: str, headers: dict, msg: str) -> RequestHandlerResult:
        self._logger.debug(f"Received POST request for {path}")
        try:
            req_obj = json.loads(msg)
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to parse a POST request body - {str(e)}")
            return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Invalid message format", content_type='text')
        try:
            parts = path.split('/')
            if parts[1] not in self._post_handlers:
                self._logger.warning(f"Endpoint {parts[1]} not found")
                return self._generate_reply(HTTPStatus.NOT_FOUND, resp_txt=f"Endpoint {parts[1]} does not exist",
                                            content_type='text')
            handler_desc = self._post_handlers[parts[1]]
            if (len(parts) - 2) < handler_desc.num_of_expected_params:
                self._logger.warning(f"For endpoint {parts[1]} expected {handler_desc.num_of_expected_params} "
                                     f"parameters but found {len(parts) - 2}")
                return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Mandatory parameter(s) required",
                                            content_type='text')
            display_id = model.NONEXISTENT_DISPLAY_ID
            if handler_desc.disp_id_req:
                display_id = self._parse_display_id(headers)
                if display_id == model.NONEXISTENT_DISPLAY_ID:
                    return self._generate_reply(HTTPStatus.BAD_REQUEST,
                                                resp_txt="Mandatory header is absent or incorrect",
                                                content_type='text')
            return handler_desc.handler(parts[2:], display_id, req_obj)
        except IndexError as e:
            self._logger.error(f"Failed to get endpoint or query parameter from the path - {str(e)}")
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _on_put(self, path: str, headers: dict, msg: str) -> RequestHandlerResult:
        self._logger.debug(f"Received PUT request for {path}")
        try:
            req_obj = json.loads(msg)
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to parse a PUT request body - {str(e)}")
            return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Invalid message format", content_type='text')
        try:
            parts = path.split('/')
            if parts[1] not in self._put_handlers:
                self._logger.warning(f"Endpoint {parts[1]} not found")
                return self._generate_reply(HTTPStatus.NOT_FOUND, resp_txt=f"Endpoint {parts[1]} does not exist",
                                            content_type='text')
            handler_desc = self._put_handlers[parts[1]]
            if (len(parts) - 2) < handler_desc.num_of_expected_params:
                self._logger.warning(f"For endpoint {parts[1]} expected {handler_desc.num_of_expected_params} "
                                     f"parameters but found {len(parts) - 2}")
                return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Mandatory parameter(s) required",
                                            content_type='text')
            display_id = model.NONEXISTENT_DISPLAY_ID
            if handler_desc.disp_id_req:
                display_id = self._parse_display_id(headers)
                if display_id == model.NONEXISTENT_DISPLAY_ID:
                    return self._generate_reply(HTTPStatus.BAD_REQUEST,
                                                resp_txt="Mandatory header is absent or incorrect",
                                                content_type='text')
            return handler_desc.handler(parts[2:], display_id, req_obj)
        except IndexError as e:
            self._logger.error(f"Failed to get endpoint or query parameter from the path - {str(e)}")
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _on_delete(self, path: str, headers: dict) -> RequestHandlerResult:
        self._logger.debug(f"Received DELETE request for {path}")
        try:
            parts = path.split('/')
            if parts[1] not in self._delete_handlers:
                self._logger.warning(f"Endpoint {parts[1]} not found")
                return self._generate_reply(HTTPStatus.NOT_FOUND, resp_txt=f"Endpoint {parts[1]} does not exist",
                                            content_type='text')
            handler_desc = self._delete_handlers[parts[1]]
            if (len(parts) - 2) < handler_desc.num_of_expected_params:
                self._logger.warning(f"For endpoint {parts[1]} expected {handler_desc.num_of_expected_params} "
                                     f"parameters but found {len(parts) - 2}")
                return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Mandatory parameter(s) required",
                                            content_type='text')
            display_id = model.NONEXISTENT_DISPLAY_ID
            if handler_desc.disp_id_req:
                display_id = self._parse_display_id(headers)
                if display_id == model.NONEXISTENT_DISPLAY_ID:
                    return self._generate_reply(HTTPStatus.BAD_REQUEST,
                                                resp_txt="Mandatory header is absent or incorrect",
                                                content_type='text')
            return handler_desc.handler(parts[2:], display_id)
        except IndexError as e:
            self._logger.error(f"Failed to get endpoint or query parameter from the path - {str(e)}")
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _on_options(self, path: str, headers: dict) -> tuple[int, dict]:
        out_headers = {"Allow": "GET, PUT, POST, OPTIONS",
                       "Access-Control-Allow-Origin": "*",
                       "Access-Control-Allow-Methods": "GET, PUT, POST, DELETE, OPTIONS",
                       "Access-Control-Allow-Headers": "Content-Type, displayId"}
        return HTTPStatus.OK, out_headers

    def _generate_reply(self, status_code: int, resp_txt: str = "", resp_obj=None,
                        content_type: str = 'none') -> RequestHandlerResult:
        headers = dict()
        headers['Access-Control-Allow-Origin'] = '*'
        if content_type == 'text' and len(resp_txt) > 0:
            headers['Content-Type'] = 'text/plain'
            headers['Content-Length'] = str(len(resp_txt))
            return status_code, headers, resp_txt
        elif content_type == 'json' and resp_obj is not None:
            headers['Content-Type'] = 'application/json'
            if type(resp_obj) == dict:
                try:
                    resp_json = json.dumps(resp_obj)
                except Exception as e:
                    self._logger.error(f"Failed to generate JSON response data - {str(e)}")
                    return HTTPStatus.INTERNAL_SERVER_ERROR, headers, ""
            elif type(resp_obj) == str:
                resp_json = resp_obj
            else:
                self._logger.error(f"Type of resp_obj {type(resp_obj)} is wrong to generate JSON response data")
                return HTTPStatus.INTERNAL_SERVER_ERROR, headers, ""
            headers['Content-Length'] = str(len(resp_json))
            return status_code, headers, resp_json
        elif content_type == 'none':
            return status_code, headers, ""
        else:
            self._logger.error(f"Failed to generate the reply for parameters: resp_text={resp_txt}, "
                               f"reso_obj={resp_obj}, content_type={content_type}")
            return HTTPStatus.INTERNAL_SERVER_ERROR, headers, ""

    def _process_get_test(self, params: list, display_id: int) -> RequestHandlerResult:
        return self._generate_reply(HTTPStatus.OK, resp_txt=f"JER Kiosk UI backend", content_type='text')

    def _process_get_brand_info(self, params: list, display_id: int) -> RequestHandlerResult:
        brand_info_path = self._data_dir.joinpath(self._config['brand_info_filename'])
        if brand_info_path.exists():
            with open(brand_info_path) as f:
                brand_info = f.read()
                return self._generate_reply(HTTPStatus.OK, resp_obj=brand_info, content_type='json')
        else:
            return self._generate_reply(HTTPStatus.NOT_FOUND)

    def _process_get_ui_model(self, params: list, display_id: int) -> RequestHandlerResult:
        ui_model_path = self._data_dir.joinpath(self._config['ui_model_filename'])
        if ui_model_path.exists():
            with open(ui_model_path) as f:
                ui_model = f.read()
                return self._generate_reply(HTTPStatus.OK, resp_obj=ui_model, content_type='json')
        else:
            return self._generate_reply(HTTPStatus.NOT_FOUND)

    def _process_get_collection(self, params: list, display_id: int) -> RequestHandlerResult:
        try:
            coll_id = int(params[0])
        except ValueError as e:
            self._logger.error(f"Failed to get collection id from the query param {params[0]}")
            return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt=f"Cannot convert {params[0]} to an ID",
                                        content_type='text')
        try:
            coll = self._db.get_collection(coll_id)
            if coll is not None:
                coll_obj = dict()
                coll_obj['id'] = coll.obj_id
                if self._lang in coll.info:
                    coll_obj['name'] = coll.info[self._lang].name
                else:
                    coll_obj['name'] = '?'
                if coll.media_id is not None:
                    coll_obj['image'] = self._config['media'] + coll.media.filename
                else:
                    coll_obj['image'] = ''
                return self._generate_reply(HTTPStatus.OK, resp_obj=coll_obj, content_type='json')
            else:
                return self._generate_reply(HTTPStatus.NOT_FOUND)
        except utils.DbError as e:
            # TODO: telemetry
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _process_get_product(self, params: list, display_id: int) -> RequestHandlerResult:
        try:
            prod_id = int(params[0])
        except ValueError as e:
            self._logger.error(f"Failed to get product id from the query param {params[0]}")
            return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt=f"Cannot convert {params[0]} to an ID",
                                        content_type='text')
        try:
            prod = self._db.get_product(prod_id)
            prod_obj = dict()
            prod_obj['id'] = prod.obj_id
            if self._lang in prod.info:
                prod_obj['name'] = prod.info[self._lang].name
                prod_obj['description'] = prod.info[self._lang].description
            else:
                prod_obj['name'] = '?'
                prod_obj['description'] = '?'
            prod_obj['variants'] = list()
            variants = self._db.get_variants(prod_id)
            for var in variants:
                var_obj = dict()
                var_obj['id'] = var.obj_id
                var_obj['price'] = var.price
                var_obj['comparePrice'] = var.price_comp
                var_obj['priceFormatted'] = var.price_fmt
                var_obj['comparePriceFormatted'] = var.price_comp_fmt
                if var.media_id is not None:
                    var_obj['image'] = self._config['media'] + var.media.filename
                else:
                    var_obj['image'] = ''
                inv_items = self._db.get_inventory_items_by_variant(var.obj_id)
                quantity = 0
                for item in inv_items:
                    quantity += item.quantity
                var_obj['available'] = quantity
                var_obj['options'] = list()
                for opt in var.options:
                    var_obj['options'].append({'name': opt.option, 'value': opt.value})
                prod_obj['variants'].append(var_obj)
            return self._generate_reply(HTTPStatus.OK, resp_obj=prod_obj, content_type='json')
        except utils.DbError as e:
            # TODO: telemetry
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _process_get_transaction_id(self, params: list, display_id: int) -> RequestHandlerResult:
        try:
            cart = self._db.get_cart_by_transaction(self._get_my_transaction_id(display_id))
            if cart is None:
                self._logger.warning(f"Trying to initiate transaction for display {display_id} but no cart is found")
                return self._generate_reply(HTTPStatus.NOT_FOUND, resp_txt="No active cart", content_type='text')
            self._tr_id_ready_flags[display_id - 1].clear()
            self._tr_id_request_data[display_id - 1].cart_id = cart.obj_id
            self._tr_id_request_data[display_id - 1].waiting = True
            self._ev_bus.post(Event(EventType.BEGIN_TRANSACTION_REQUEST, {'cart_id': cart.obj_id}))
            res = self._tr_id_ready_flags[display_id - 1].wait(self._config['transaction_id_timeout'])
            self._tr_id_request_data[display_id - 1].waiting = False
            if res and self._tr_id_request_data[display_id - 1].success:
                cart = self._db.get_cart(self._tr_id_request_data[display_id - 1].cart_id)
                resp = {'transactionId': cart.transaction_id}
                return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')
            return self._generate_reply(HTTPStatus.SERVICE_UNAVAILABLE, resp_txt="Failed to get transaction id",
                                        content_type='text')
        except utils.DbError as e:
            # TODO: telemetry
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)
        except Exception as e:
            # TODO: telemetry
            self._logger.error(f"Got unexpected exception - {str(e)}")
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)

    def _process_put_cart(self, params: list, display_id: int, req_obj: dict) -> RequestHandlerResult:
        try:
            res, msg = self._cart_logic.update(self._get_my_transaction_id(display_id), display_id,
                                               model.CartType.LOCAL, req_obj['variantId'], req_obj['amount'])
            if res == CartOperationResult.OK:
                resp = {'message': 'OK'}
                return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')
            elif res == CartOperationResult.NOK:
                resp = {'message': 'NOK'}
                return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')
            elif res == CartOperationResult.ERROR:
                return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt=msg, content_type='text')
            else:
                return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)
        except KeyError as e:
            self._logger.error(f"Unable to locate expected cart parameters - {str(e)}")
            return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Invalid request format", content_type='text')

    def _process_delete_cart(self, params: list, display_id: int) -> RequestHandlerResult:
        # At the moment it is only supported when transaction ID is not assigned yet
        self._cart_logic.clear(self._get_my_transaction_id(display_id))
        resp = {'message': 'OK'}
        return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')

    def _process_post_pickup(self, params: list, display_id: int, req_obj: dict) -> RequestHandlerResult:
        try:
            pickup_code = req_obj['code']
            self._logger.debug(f"Processing pickup for display {display_id}, code {pickup_code}")
            carts = self._db.get_carts(pickup_code)
            if len(carts) == 0:
                self._logger.info("No active carts found for the given pickup code")
                status = "NOT FOUND"
                # Try to look up in the order history
                order_hist_records = self._db.get_order_history_records(pickup_code)
                if len(order_hist_records) > 0:
                    # Normally, only single entry should be found.
                    # There is no way to choose a correct entry if there are more than one
                    # So, always take the first one
                    if order_hist_records[0].completion_status == model.ReservationCompletionStatus.DISPENSED:
                        status = "FULFILLED"
                    else:
                        status = "EXPIRED"
                else:
                    self._logger.info("No entries found in order history for the given pickup code")
                resp = {'status': status}
                return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')
            else:
                # Normally, only single cart should be found.
                # There is no way to choose a correct cart if there are more than one
                # So, always take the first one
                cart = carts[0]
                self._logger.debug(f"Initiating dispensing for remote cart {cart.obj_id}, transaction "
                                   f"{cart.transaction_id}, order {cart.order_info}")
                res, errmsg = self._cart_logic.dispense(cart.transaction_id, display_id)
                if res == CartOperationResult.OK or res == CartOperationResult.PENDING:
                    resp = dict()
                    resp['status'] = "OK" if res == CartOperationResult.OK else "PENDING"
                    resp['order'] = list()
                    cart_contents = self._db.get_cart_items(cart.obj_id)
                    for item in cart_contents:
                        var = self._db.get_variant(item.variant_id)
                        prod = self._db.get_product(var.prod_id)
                        var_name = ""
                        if self._lang in prod.info:
                            var_name = prod.info[self._lang].name
                        if self._lang in var.info and var.info[self._lang].name != var_name:
                            var_name = f"{var_name} ({var.info[self._lang].name})"
                        var_obj = dict()
                        var_obj['variantId'] = var.obj_id
                        var_obj['name'] = var_name
                        var_obj['image'] = ''
                        if var.media_id is not None:
                            var_obj['image'] = self._config['media'] + var.media.filename
                        var_obj['amount'] = item.amount
                        resp['order'].append(var_obj)
                    return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')
                elif res == CartOperationResult.NOK or res == CartOperationResult.ERROR:
                    resp = {'status': 'NOK'}
                    return self._generate_reply(HTTPStatus.OK, resp_obj=resp, content_type='json')
                else:
                    return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)
        except KeyError as e:
            self._logger.error(f"Unable to locate pickup code - {str(e)}")
            return self._generate_reply(HTTPStatus.BAD_REQUEST, resp_txt="Invalid request format", content_type='text')
        except utils.DbError as e:
            # TODO: telemetry
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)
        except Exception as e:
            # TODO: telemetry
            self._logger.error(f"Got unexpected exception - {str(e)}")
            return self._generate_reply(HTTPStatus.INTERNAL_SERVER_ERROR)
