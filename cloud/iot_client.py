from abc import abstractmethod
from core.appmodule import AppModule
from core.logger import Logger


class IotClient(AppModule):
    """Interface, realization of which should implement connection logic to the cloud."""

    MYNAME = 'cloud.iot'
    CONNECT_ATTEMPTS = 3
    CONNECT_TIMEOUT = 5

    def __init__(self, config_data: dict, logger: Logger):
        super().__init__(IotClient.MYNAME, config_data, logger)
        self._on_receive = None
        self._handlers = dict()

    @abstractmethod
    def connect(self):
        """Initiates connection with the server"""
        pass

    @abstractmethod
    def disconnect(self):
        """Performs disconnection from the server"""
        pass

    @abstractmethod
    def run(self):
        """By any means should create a loop to process network events."""
        pass

    def register_handler(self, topic: str, handler):
        """Register handler with expected signature f(message: str) for a topic"""
        self._handlers[topic] = handler
