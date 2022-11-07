from core.appmodule import AppModuleWithEvents, AppModuleEventType
from core.logger import Logger
from core.event_bus import EventBus, Event
from logic.planogram import PlanogramLogic
from core.events import EventType
from db.model import MachineState
from core.fsm import FSM
from enum import auto, unique


@unique
class MachineEventType(AppModuleEventType):
    HW_IS_READY = auto()
    DOOR_STATE_CHANGED = auto()
    PLANOGRAM_UPDATED = auto()


# Events structure:
# HW_IS_READY
#   no fields
#
# DOOR_STATE_CHANGED
#   'open': bool
#
# PLANOGRAM_UPDATED
#   no fields
#


class MachineLogic(AppModuleWithEvents):
    """Implements logic related to machine management.
    """
    MYNAME = 'logic.machine'
    REQ_CFG_OPTIONS = []

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus, planogram_logic: PlanogramLogic):
        super().__init__(MachineLogic.MYNAME, config_data, logger)
        self._ev_bus = ev_bus
        self._planogram_logic = planogram_logic
        self._fsm = FSM(logger)
        self._dispenser_is_ready = False
        self._is_door_open = False
        self._is_hw_error_indicated = False
        self._is_dispensing_in_progress = False

    def _get_my_required_cfg_options(self) -> list:
        return MachineLogic.REQ_CFG_OPTIONS

    def start(self):
        super().start()
        self._ev_bus.subscribe(EventType.HW_DISPENSER_IS_READY, self._app_event_handler)
        self._ev_bus.subscribe(EventType.DOOR_STATE_CHANGED, self._app_event_handler)
        self._ev_bus.subscribe(EventType.PLANOGRAM_UPDATE_DONE, self._app_event_handler)
        # TODO: process events about dispensing begin, dispensing end, hw_defect
        self._register_ev_handler(MachineEventType.HW_IS_READY, self._on_hw_ready)
        self._register_ev_handler(MachineEventType.DOOR_STATE_CHANGED, self._on_door_state_changed)
        self._register_ev_handler(MachineEventType.PLANOGRAM_UPDATED, self._on_planogram_updated)
        self._logger.info("Machine Logic module started")

    def stop(self):
        super().stop()
        self._logger.info("Machine Logic module stopped")

    def _init_fsm(self):
        self._fsm.add_state(MachineState.STARTUP, 'STARTUP', on_exit=self._on_startup_complete, is_initial=True)
        self._fsm.add_state(MachineState.AVAILABLE, 'AVAILABLE', on_enter=self._on_state_changed)
        self._fsm.add_state(MachineState.UNAVAILABLE, 'UNAVAILABLE', on_enter=self._on_state_changed)
        self._fsm.add_state(MachineState.BUSY, 'BUSY', on_enter=self._on_state_changed)
        self._fsm.add_state(MachineState.MAINTENANCE, 'MAINTENANCE', on_enter=self._on_state_changed)
        self._fsm.add_state(MachineState.ERROR, 'ERROR', on_enter=self._on_state_changed)
        self._fsm.add_state(MachineState.UPDATE, 'UPDATE', on_enter=self._on_state_changed)
        self._fsm.add_transition(MachineState.STARTUP, MachineState.AVAILABLE, self._check_available_condition)
        self._fsm.add_transition(MachineState.STARTUP, MachineState.UNAVAILABLE, self._check_unavailable_condition)
        self._fsm.add_transition(MachineState.STARTUP, MachineState.MAINTENANCE, self._check_maintenance_condition)
        self._fsm.add_transition(MachineState.STARTUP, MachineState.ERROR, self._check_error_condition)
        self._fsm.add_transition(MachineState.AVAILABLE, MachineState.UNAVAILABLE, self._check_unavailable_condition)
        self._fsm.add_transition(MachineState.AVAILABLE, MachineState.BUSY, self._check_busy_condition)
        self._fsm.add_transition(MachineState.AVAILABLE, MachineState.MAINTENANCE, self._check_maintenance_condition)
        self._fsm.add_transition(MachineState.AVAILABLE, MachineState.ERROR, self._check_error_condition)
        self._fsm.add_transition(MachineState.AVAILABLE, MachineState.UPDATE, self._check_update_condition)
        self._fsm.add_transition(MachineState.UNAVAILABLE, MachineState.AVAILABLE, self._check_available_condition)
        self._fsm.add_transition(MachineState.UNAVAILABLE, MachineState.MAINTENANCE, self._check_maintenance_condition)
        self._fsm.add_transition(MachineState.UNAVAILABLE, MachineState.ERROR, self._check_error_condition)
        self._fsm.add_transition(MachineState.UNAVAILABLE, MachineState.UPDATE, self._check_update_condition)
        self._fsm.add_transition(MachineState.BUSY, MachineState.AVAILABLE, self._check_available_condition)
        self._fsm.add_transition(MachineState.BUSY, MachineState.ERROR, self._check_error_condition)
        self._fsm.add_transition(MachineState.MAINTENANCE, MachineState.AVAILABLE, self._check_available_condition)
        self._fsm.add_transition(MachineState.MAINTENANCE, MachineState.UNAVAILABLE, self._check_unavailable_condition)
        self._fsm.add_transition(MachineState.MAINTENANCE, MachineState.ERROR, self._check_error_condition)
        self._fsm.add_transition(MachineState.ERROR, MachineState.AVAILABLE, self._check_available_condition)
        self._fsm.add_transition(MachineState.ERROR, MachineState.MAINTENANCE, self._check_maintenance_condition)
        self._fsm.add_transition(MachineState.ERROR, MachineState.UPDATE, self._check_error_condition)

    def _app_event_handler(self, ev: Event):
        """Processes external events"""
        if ev.type == EventType.HW_DISPENSER_IS_READY:
            self._dispenser_is_ready = True
            self._put_event(MachineEventType.HW_IS_READY, {})
        elif ev.type == EventType.DOOR_STATE_CHANGED:
            self._put_event(MachineEventType.DOOR_STATE_CHANGED, ev.body)
        elif ev.type == EventType.PLANOGRAM_UPDATE_DONE:
            self._put_event(MachineEventType.PLANOGRAM_UPDATED, {})

    def _on_hw_ready(self, params: dict):
        self._fsm.run()

    def _on_door_state_changed(self, params: dict):
        try:
            self._is_door_open = params['open']
            self._fsm.run()
        except KeyError as e:
            self._logger.error(f"Failed to access data structures - {str(e)}")

    def _on_planogram_updated(self, params: dict):
        self._fsm.run()

    def _on_state_changed(self):
        self._ev_bus.post(Event(EventType.MACHINE_STATE_CHANGED, {'state': self._fsm.get_current_state()}))

    def _on_startup_complete(self):
        self._ev_bus.post(Event(EventType.STARTUP_COMPLETE, {}))

    def _check_available_condition(self) -> bool:
        return (self._planogram_logic.is_planogram_set() and
                self._dispenser_is_ready and
                not self._is_door_open and
                not self._is_hw_error_indicated and
                not self._is_dispensing_in_progress)

    def _check_unavailable_condition(self) -> bool:
        return (not self._planogram_logic.is_planogram_set() and
                self._dispenser_is_ready and
                not self._is_door_open and
                not self._is_hw_error_indicated and
                not self._is_dispensing_in_progress)

    def _check_busy_condition(self) -> bool:
        return self._is_dispensing_in_progress

    def _check_maintenance_condition(self) -> bool:
        return self._is_door_open

    def _check_error_condition(self) -> bool:
        return self._is_hw_error_indicated and not self._is_door_open

    def _check_update_condition(self) -> bool:
        # TODO
        return False
