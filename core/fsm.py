from enum import IntEnum
from collections.abc import Callable
from core.logger import Logger


class FSMStatesBase(IntEnum):
    pass


OnEnterHandlerT = Callable[[], None]
OnExitHandlerT = Callable[[], None]
ConditionCheckerT = Callable[[], bool]


class FSMTransition:
    def __init__(self, to_state: FSMStatesBase, cond_checker: ConditionCheckerT):
        self._to_state = to_state
        self._cond_checker = cond_checker

    def should_transition(self) -> (bool, FSMStatesBase | None):
        if self._cond_checker and self._cond_checker():
            return True, self._to_state
        return False, None


class FSMState:
    def __init__(self, logger: Logger, name: str, on_enter: OnEnterHandlerT = None, on_exit: OnExitHandlerT = None):
        self._logger = logger
        self._name = name
        self._on_enter = on_enter
        self._on_exit = on_exit
        self._transitions: list[FSMTransition] = list()

    def add_transition(self, transition: FSMTransition):
        self._transitions.append(transition)

    def do_enter(self):
        self._logger.debug(f"State {self._name} entered")
        if self._on_enter:
            self._on_enter()

    def do_exit(self):
        self._logger.debug(f"State {self._name} exited")
        if self._on_exit:
            self._on_exit()

    def get_next_state(self) -> FSMStatesBase | None:
        """Called by FSM. Walks through all the configured transitions and check if any of them should be activated.
           Returns state, which FSM should transition to, or None if no transition is possible.
        """
        for transition in self._transitions:
            shall_activate, to_state = transition.should_transition()
            if shall_activate:
                return to_state
        return None


class FSM:
    def __init__(self, logger: Logger):
        self._logger = logger
        self._states: dict[FSMStatesBase, FSMState] = dict()
        self._current_state: FSMStatesBase | None = None

    def add_state(self, state: FSMStatesBase, name: str, on_enter: OnEnterHandlerT = None,
                  on_exit: OnExitHandlerT = None, is_initial: bool = False):
        fsm_state = FSMState(self._logger, name, on_enter, on_exit)
        self._states[state] = fsm_state
        if is_initial:
            self._current_state = state

    def add_transition(self, from_state: FSMStatesBase, to_state: FSMStatesBase, cond_checker: ConditionCheckerT):
        fsm_transition = FSMTransition(to_state, cond_checker)
        fsm_state = self._states.get(from_state, None)
        if fsm_state is None:
            self._logger.warning(f"Failed to add transition for non-existent state {from_state}")
            return
        fsm_state.add_transition(fsm_transition)

    def run(self):
        if self._current_state is None:
            self._logger.warning("Initial state was not defined")
            return
        next_state = self._states[self._current_state].get_next_state()
        if next_state is not None:
            if next_state not in self._states:
                self._logger.error(f"Cannot transition to non-existent state {next_state}")
                return
            self._states[self._current_state].do_exit()
            self._states[next_state].do_enter()
            self._current_state = next_state

    def get_current_state(self) -> FSMStatesBase:
        return self._current_state
