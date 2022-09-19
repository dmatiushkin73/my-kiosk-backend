from abc import abstractmethod
from pathlib import Path
from core.appmodule import AppModule
from core.logger import Logger
from core.events import EventType
from core.event_bus import Event, EventBus
from cloud.iot_client import IotClient
from core import utils


class CloudClient(AppModule):
    """Base class to be subclassed to implement interface to the Cloud"""
    def __init__(self, module: str, config_data: dict, logger: Logger, ev_bus: EventBus):
        super().__init__(module, config_data, logger)
        self._event_bus = ev_bus
        self._iot_client: IotClient = None

    def get_iot_client(self) -> IotClient:
        return self._iot_client

    def start(self):
        if self._iot_client:
            self._iot_client.start()
        self._event_bus.subscribe(EventType.SEND_TO_CLOUD, self._event_handler)

    def stop(self):
        if self._iot_client:
            self._iot_client.stop()

    def run(self):
        if self._iot_client:
            self._iot_client.run()

    @abstractmethod
    def invoke_api_get(self, name: str, params: dict) -> dict:
        """Invokes method of the API class 'name' that performs GET request to the Cloud and returns the response"""
        return {}

    @abstractmethod
    def invoke_api_post(self, name: str, obj: dict):
        """Invokes method of the API class 'name' that performs POST request to the Cloud"""
        pass

    @abstractmethod
    def invoke_api_post_with_response(self, name: str, obj: dict) -> dict:
        """Invokes method of the API class 'name' that performs POST request to the cloud and returns response"""
        return {}

    @abstractmethod
    def download_image(self, url: str, save_to: Path) -> str:
        """Downloads an image from the cloud datastore. Returns name of the file"""
        return ''

    def _event_handler(self, ev: Event):
        if ev.type == EventType.SEND_TO_CLOUD:
            try:
                self.invoke_api_post(ev.body['api'], ev.body['data'])
            except utils.CloudApiNotFound:
                self._logger.error(f"API {ev.body['api']} is not configured")
            except utils.CloudApiServerError as e:
                self._logger.error(f"Server returned status code {e.status_code} with message({e.response}) for "
                                   f"API {ev.body['api']}")
            except utils.CloudApiConnectionError as e:
                self._logger.error(f"Unable to connect to the server, error - ({e.msg}) for API {ev.body['api']}")
            except utils.CloudApiTimeoutError:
                self._logger.error(f"Timeout error occurred while trying to use API {ev.body['api']}")
