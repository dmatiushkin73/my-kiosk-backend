from core.logger import Logger
from core.appmodule import AppModule
from core.events import EventType
from collections import deque
from threading import Lock, Timer


class Event:
    def __init__(self, evtype: EventType, evbody: dict):
        self.type = evtype
        self.body = evbody


class EventBus(AppModule):
    """Implements infrastructure for passing events among modules using subscriber pattern"""
    MYNAME = 'core.evtbus'
    REQ_CFG_OPTIONS = []
    DISPATCH_PERIOD = 0.1
    MAX_NUM_LOW_PRIO_EVENTS_PER_TIME = 5
    MAX_NUM_NORMAL_PRIO_EVENTS_PER_TIME = 10
    MAX_NUM_HIGH_PRIO_EVENTS_PER_TIME = 15

    def __init__(self, config_data: dict, logger: Logger):
        super().__init__(EventBus.MYNAME, config_data, logger)
        self._lowprio_q = deque()
        self._lowprio_q_lock = Lock()
        self._normprio_q = deque()
        self._normprio_q_lock = Lock()
        self._highprio_q = deque()
        self._highprio_q_lock = Lock()
        self._subscriptions = dict()
        self._proc_timer = None

    def _get_my_required_cfg_options(self) -> list:
        return EventBus.REQ_CFG_OPTIONS

    def start(self):
        self._proc_timer = Timer(EventBus.DISPATCH_PERIOD, self._dispatch)

    def stop(self):
        if self._proc_timer:
            self._proc_timer.cancel()

    def subscribe(self, evtype: EventType, handler):
        """Register a handler with signature f(ev: Event) for event type"""
        if evtype not in self._subscriptions:
            self._subscriptions[evtype] = list()
        self._subscriptions[evtype].append(handler)

    def post_low(self, event: Event):
        """Push event to the queue with low priority"""
        with self._lowprio_q_lock:
            self._lowprio_q.appendleft(event)

    def post(self, event: Event):
        """Push event to the queue with normal priority"""
        with self._normprio_q_lock:
            self._normprio_q.appendleft(event)

    def post_high(self, event: Event):
        """Push event to the queue with high priority"""
        with self._highprio_q_lock:
            self._highprio_q.appendleft(event)

    def _dispatch(self):
        """Takes events from the low, normal and high priority queues and invokes corresponding registered handlers"""
        self._highprio_q_lock.acquire()
        for _ in range(EventBus.MAX_NUM_HIGH_PRIO_EVENTS_PER_TIME):
            if len(self._highprio_q) > 0:
                event: Event = self._highprio_q.pop()
                if event.type in self._subscriptions:
                    for handler in self._subscriptions[event.type]:
                        self._highprio_q_lock.release()
                        handler(event)
                        self._highprio_q_lock.acquire()
            else:
                break
        self._highprio_q_lock.release()

        self._normprio_q_lock.acquire()
        for _ in range(EventBus.MAX_NUM_NORMAL_PRIO_EVENTS_PER_TIME):
            if len(self._normprio_q) > 0:
                event: Event = self._normprio_q.pop()
                if event.type in self._subscriptions:
                    for handler in self._subscriptions[event.type]:
                        self._normprio_q_lock.release()
                        handler(event)
                        self._normprio_q_lock.acquire()
            else:
                break
        self._normprio_q_lock.release()

        self._lowprio_q_lock.acquire()
        for _ in range(EventBus.MAX_NUM_LOW_PRIO_EVENTS_PER_TIME):
            if len(self._lowprio_q) > 0:
                event: Event = self._lowprio_q.pop()
                if event.type in self._subscriptions:
                    for handler in self._subscriptions[event.type]:
                        self._lowprio_q_lock.release()
                        handler(event)
                        self._lowprio_q_lock.acquire()
        self._lowprio_q_lock.release()
        self._proc_timer = Timer(EventBus.DISPATCH_PERIOD, self._dispatch)
