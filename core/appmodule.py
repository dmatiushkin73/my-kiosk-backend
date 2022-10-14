from abc import ABC, abstractmethod
from enum import Enum, auto, unique
from threading import Thread, Condition
from collections import deque
from collections.abc import Callable
from copy import deepcopy
from core.logger import Logger
from core.utils import check_config, ConfigError


@unique
class AppModuleEventType(Enum):
    pass


class AppModuleEvent:
    def __init__(self, ev_type: AppModuleEventType, ev_body: dict):
        self.type: AppModuleEventType = ev_type
        self.body: dict = ev_body


class AppModule(ABC):
    """Base class for all application modules"""
    def __init__(self, modname: str, config_data: dict, logger: Logger):
        self._my_name = modname
        self._config = config_data
        self._logger = logger.get_logger(self._my_name)

    @abstractmethod
    def _get_my_required_cfg_options(self) -> list:
        """Returns list of module's required configuration option names"""
        return []

    def validate_config(self):
        """Validates configuration passed to the module, raises exception ConfigError in case of failed validation"""
        res, opt = check_config(self._config, self._get_my_required_cfg_options())
        if not res:
            raise ConfigError(self._my_name, opt)

    @abstractmethod
    def start(self):
        """Will be first after creation and successful validation of the config"""
        pass

    @abstractmethod
    def stop(self):
        """Will be called on application stop"""
        pass


EventHandlerT = Callable[[dict], None]


class AppModuleWithEvents(AppModule):
    """Base class for application modules that require triggering and processing of internal events"""
    def __init__(self, modname: str, config_data: dict, logger: Logger):
        super().__init__(modname, config_data, logger)
        self._event_q: deque[AppModuleEvent | None] = deque()
        self._cv: Condition = Condition()
        self._event_thread: Thread = Thread(target=self._event_processing_worker)
        self._stopped = False
        self._ev_handlers: dict[AppModuleEventType, EventHandlerT] = dict()

    def start(self):
        super().start()
        self._event_thread.start()

    def stop(self):
        self._stopped = True
        with self._cv:
            self._event_q.appendleft(None)
            self._cv.notify()
        self._event_thread.join()
        super().stop()

    def _event_processing_worker(self):
        """Processes internal events in a separate thread"""
        while not self._stopped:
            with self._cv:
                self._cv.wait_for(lambda: len(self._event_q) > 0)
                ev = self._event_q.pop()
                if ev is not None and ev.type in self._ev_handlers:
                    self._ev_handlers[ev.type](ev.body)

    def _register_ev_handler(self, ev_type: AppModuleEventType, handler: EventHandlerT):
        self._ev_handlers[ev_type] = handler

    def _put_event(self, ev_type: AppModuleEventType, ev_body: dict):
        with self._cv:
            self._event_q.appendleft(AppModuleEvent(ev_type, deepcopy(ev_body)))
            self._cv.notify()
