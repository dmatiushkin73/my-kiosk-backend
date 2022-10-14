from core.appmodule import AppModuleWithEvents, AppModuleEventType
from core.logger import Logger
from core.event_bus import EventBus, Event
from core.events import EventType
from db import model
from db.database import Database
from core import utils
import json
from enum import auto, unique
from threading import Thread, Timer
import websockets
import asyncio
from concurrent.futures import wait


@unique
class WsEventType(AppModuleEventType):
    MACHINE_STATE_CHANGED = auto()
    BRAND_INFO_UPDATED = auto()
    UI_MODEL_UPDATED = auto()
    DISPENSING_STATUS = auto()
    HUMAN_DETECTED = auto()


# Events structure:
# MACHINE_STATE_CHANGED
#   'state': model.MachineState
#
# BRAND_INFO_UPDATED
#   no fields
#
# UI_MODEL_UPDATED
#   no fields
#
# DISPENSING_STATUS
#   'cart_id': int
#   'unit_id': int
#   'location': int
#   'variant_id': int
#   'status': model.DispensingStatus
#
# HUMAN_DETECTED
#   'display_id': int
#   'profile_id': int
#


class BackendWebsocketServer(AppModuleWithEvents):
    """Handles WebSocket connections with the Kiosk UI"""
    MYNAME = 'ui.ws'
    REQ_CFG_OPTIONS = ['port', 'keep_alive_interval', 'keep_alive_log']
    CONN_KEY = '?displayId='
    MACHINE_STATE_STR = {model.MachineState.STARTUP: 'startup',
                         model.MachineState.AVAILABLE: 'available',
                         model.MachineState.UNAVAILABLE: 'unavailable',
                         model.MachineState.BUSY: 'busy',
                         model.MachineState.MAINTENANCE: 'maintenance',
                         model.MachineState.ERROR: 'error',
                         model.MachineState.UPDATE: 'sw-update'}
    DISPENSING_STATUS_STR = {model.DispensingStatus.STARTED_ONE_ITEM: 'dispensing_started',
                             model.DispensingStatus.FINISHED_ONE_ITEM: 'dispensed_one_item',
                             model.DispensingStatus.ERROR_ONE_ITEM: 'dispensed_one_item',
                             model.DispensingStatus.WAITING_FOR_PICKUP: 'wait_for_pickup',
                             model.DispensingStatus.COMPLETED: 'dispensed_all_items'}

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus, db: Database):
        super().__init__(BackendWebsocketServer.MYNAME, config_data, logger)
        self._ev_bus = ev_bus
        self._db = db
        self._work_thread: Thread = None
        self._ws_server: websockets.WebSocketServer = None
        self._connections: dict[int, websockets.WebSocketServerProtocol] = dict()
        self._machine_state: model.MachineState = model.MachineState.STARTUP
        self._keepalive_tm = None
        self._async_loop = None
        self._last_dispensing_status: dict[int, model.DispensingStatus] = dict()

    def _get_my_required_cfg_options(self) -> list:
        return BackendWebsocketServer.REQ_CFG_OPTIONS

    def start(self):
        super().start()
        self._work_thread = Thread(target=self._start_service)
        self._work_thread.start()
        for display_id in range(1, model.MAX_DISPLAYS):
            self._last_dispensing_status[display_id] = model.DispensingStatus.COMPLETED
        self._keepalive_tm = Timer(self._config['keep_alive_interval'], self._keepalive_tm_handler)
        self._ev_bus.subscribe(EventType.MACHINE_STATE_CHANGED, self._app_event_handler)
        self._ev_bus.subscribe(EventType.BRAND_INFO_UPDATED, self._app_event_handler)
        self._ev_bus.subscribe(EventType.UI_MODEL_UPDATED, self._app_event_handler)
        self._ev_bus.subscribe(EventType.DISPENSING_STATUS, self._app_event_handler)
        self._ev_bus.subscribe(EventType.HUMAN_DETECTED, self._app_event_handler)
        self._register_ev_handler(WsEventType.MACHINE_STATE_CHANGED, self._process_machine_state_changed)
        self._register_ev_handler(WsEventType.BRAND_INFO_UPDATED, self._process_brand_info_updated)
        self._register_ev_handler(WsEventType.UI_MODEL_UPDATED, self._process_ui_model_updated)
        self._register_ev_handler(WsEventType.DISPENSING_STATUS, self._process_dispensing_status)
        self._register_ev_handler(WsEventType.HUMAN_DETECTED, self._process_human_detected)

    def stop(self):
        if self._keepalive_tm:
            self._keepalive_tm.cancel()
        super().stop()
        if self._ws_server and self._ws_server.is_serving():
            future = asyncio.run_coroutine_threadsafe(self._close_server(), self._async_loop)
            wait((future,))
        if self._async_loop:
            self._async_loop.close()
        self._logger.info(f"Websocket server on port {self._config['port']} shut down")
        if self._work_thread:
            self._async_loop.stop()
            self._work_thread.join()

    async def _server_co(self):
        self._ws_server = await websockets.serve(self._connection_handler, port=self._config['port'])
        await self._ws_server.serve_forever()

    async def _close_server(self):
        self._ws_server.close()
        await self._ws_server.wait_closed()

    def _start_service(self):
        """Executes Websocket server loop and blocks on it"""
        self._logger.info(f"Starting Websocket server on port {self._config['port']}")
        self._async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._async_loop)
        try:
            self._async_loop.run_until_complete(self._server_co())
        except asyncio.CancelledError:
            self._async_loop.run_until_complete(self._close_server())

    async def _connection_handler(self, ws: websockets.WebSocketServerProtocol):
        """Called by the Websocket Server when a new connection is established"""
        s_before, s_sep, s_after = ws.path.rpartition(BackendWebsocketServer.CONN_KEY)
        if s_sep == '':
            self._logger.warning(f"Websocket connection path ({ws.path}) does not contain required parameter")
            return
        try:
            display_id = int(s_after)
        except ValueError as e:
            self._logger.error(f"Failed to get display id from the websocket connection path ({ws.path}) - {str(e)}")
            return
        if display_id == model.NONEXISTENT_DISPLAY_ID or display_id > model.MAX_DISPLAYS:
            self._logger.warning(f"Invalid display id received - {display_id}")
            return
        self._connections[display_id] = ws
        self._logger.info(f"Established Websocket connection with Kiosk UI display id {display_id}")
        try:
            async for msg in ws:
                # At the moment Kiosk UI does not send anything
                pass
        except websockets.ConnectionClosedError:
            pass
        del self._connections[display_id]
        self._logger.info(f"Closed Websocket connection with Kiosk UI display id {display_id}")

    def _keepalive_tm_handler(self):
        if self._machine_state != model.MachineState.STARTUP:
            self._send_machine_state()
        self._keepalive_tm = Timer(self._config['keep_alive_interval'], self._keepalive_tm_handler)

    def _app_event_handler(self, ev: Event):
        """Processes external events"""
        if ev.type == EventType.MACHINE_STATE_CHANGED:
            self._put_event(WsEventType.MACHINE_STATE_CHANGED, ev.body)
        elif ev.type == EventType.BRAND_INFO_UPDATED:
            self._put_event(WsEventType.BRAND_INFO_UPDATED, {})
        elif ev.type == EventType.UI_MODEL_UPDATED:
            self._put_event(WsEventType.UI_MODEL_UPDATED, {})
        elif ev.type == EventType.DISPENSING_STATUS:
            self._put_event(WsEventType.DISPENSING_STATUS, ev.body)
        elif ev.type == EventType.HUMAN_DETECTED:
            self._put_event(WsEventType.HUMAN_DETECTED, ev.body)

    def _process_machine_state_changed(self, params: dict):
        try:
            self._machine_state = params['state']
            self._send_machine_state()
        except KeyError as e:
            self._logger.error(f"Failed to access input parameter - {str(e)}")

    def _process_brand_info_updated(self, params: dict):
        msg = {'messageType': 'brandInfoUpdated'}
        self._broadcast_msg(msg)

    def _process_ui_model_updated(self, params: dict):
        msg = {'messageType': 'uiModelUpdated'}
        self._broadcast_msg(msg)

    def _process_dispensing_status(self, params: dict):
        try:
            cart_id = params['cart_id']
            variant_id = params['variant_id']
            status = params['status']
            cart = self._db.get_cart(cart_id)
            if cart is None:
                self._logger.error(f"Failed to find cart {cart_id}")
                return
            if (cart.checkout_method == model.CheckoutMethod.LOCAL or
                    cart.checkout_method == model.CheckoutMethod.PICKUP):
                last_status = self._last_dispensing_status[cart.display_id]
                self._logger.debug(f"Dispensing status for cart {cart_id} display {cart.display_id} changed from "
                                   f"status {last_status} to status {status}")
                msg = dict()
                msg['messageType'] = 'dispensingStatus'
                msg['eventType'] = BackendWebsocketServer.DISPENSING_STATUS_STR[status]
                if status == model.DispensingStatus.STARTED_ONE_ITEM:
                    if (last_status == model.DispensingStatus.COMPLETED or
                            last_status == model.DispensingStatus.WAITING_FOR_PICKUP):
                        self._send_msg(cart.display_id, msg)
                elif status == model.DispensingStatus.FINISHED_ONE_ITEM:
                    msg['status'] = True
                    msg['variantId'] = variant_id
                    self._send_msg(cart.display_id, msg)
                elif status == model.DispensingStatus.ERROR_ONE_ITEM:
                    msg['status'] = False
                    msg['variantId'] = variant_id
                elif status == model.DispensingStatus.WAITING_FOR_PICKUP:
                    self._send_msg(cart.display_id, msg)
                elif status == model.DispensingStatus.COMPLETED:
                    self._send_msg(cart.display_id, msg)
                else:
                    self._logger.warning(f"Unexpected dispensing status: {status}")
                    return
                self._last_dispensing_status[cart.display_id] = status
        except KeyError as e:
            self._logger.error(f"Failed to access input parameter - {str(e)}")
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _process_human_detected(self, params: dict):
        try:
            display_id = params['display_id']
            if display_id <= model.NONEXISTENT_DISPLAY_ID or display_id > model.MAX_DISPLAYS:
                self._logger.warning(f"Invalid display id: {display_id}")
                return
            msg = dict()
            msg['messageType'] = 'humanDetected'
            msg['profileId'] = params['profileId']
            self._send_msg(display_id, msg)
        except KeyError as e:
            self._logger.error(f"Failed to access input parameter - {str(e)}")

    def _send_machine_state(self):
        msg = dict()
        msg['messageType'] = "machineStatus"
        msg['status'] = BackendWebsocketServer.MACHINE_STATE_STR[self._machine_state]
        self._broadcast_msg(msg, log=True if self._config['keep_alive_log'] else False)

    def _send_msg(self, display_id: int, msg: dict, log: bool = True):
        if display_id not in self._connections:
            return
        out_str = json.dumps(msg)
        try:
            future = asyncio.run_coroutine_threadsafe(self._connections[display_id].send(out_str), self._async_loop)
            wait((future,))
            if log:
                self._logger.debug(f"Sent message to Kiosk UI display {display_id} - ({out_str})")
        except websockets.ConnectionClosed:
            self._logger.warning(f"Failed to send message to Kiosk UI display {display_id} "
                                 "due to closed connection")
        except TypeError as e:
            self._logger.error(f"Failed to send message to Kiosk UI display {display_id} - {str(e)}")

    def _broadcast_msg(self, msg: dict, log: bool = True):
        for display_id in self._connections.keys():
            self._send_msg(display_id, msg, log)
