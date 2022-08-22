from core.appmodule import AppModule
from core.logger import Logger
from cloud.mqtt_client import MqttClient
from cloud.aws_api import AwsApi
from core.utils import DEVICE_ID_PLACEHOLDER, CUSTOMER_ID_PLACEHOLDER, CloudApiNotFound, get_name_from_url
from core.utils import CloudApiImageDownloadError, CloudApiConnectionError, CloudApiTimeoutError, CloudApiServerError
from core.events import EventType
from core.event_bus import Event, EventBus
from pathlib import Path
import requests


class AwsClient(AppModule):
    """Implements access to AWS cloud"""
    MYNAME = 'cloud.api'
    REQ_CFG_OPTIONS = ['deviceId', 'customerId', 'iot', 'api_endpoints']

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus):
        super().__init__(AwsClient.MYNAME, config_data, logger)
        self._event_bus = ev_bus
        self._device_id = ''
        self._customer_id = ''
        self._iot_client = MqttClient(self._config.get('iot', {}), logger)
        self._endpoints = dict()

    def _get_my_required_cfg_options(self) -> list:
        return AwsClient.REQ_CFG_OPTIONS

    def validate_config(self):
        super().validate_config()
        self._iot_client.validate_config()

    def start(self):
        self._device_id = self._config['deviceId']
        self._customer_id = self._config['customerId']
        self._iot_client.set_params(self._device_id, self._customer_id)
        self._iot_client.start()
        for endpoint in self._config['api_endpoints']:
            if DEVICE_ID_PLACEHOLDER in endpoint['value']:
                endpoint['value'] = endpoint['value'].replace(DEVICE_ID_PLACEHOLDER, self._device_id)
            if CUSTOMER_ID_PLACEHOLDER in endpoint['value']:
                endpoint['value'] = endpoint['value'].replace(CUSTOMER_ID_PLACEHOLDER, self._customer_id)
            self._endpoints[endpoint['name']] = AwsApi(endpoint['key'], endpoint['value'])
        self._event_bus.subscribe(EventType.SEND_TO_CLOUD, self._event_handler)

    def stop(self):
        self._iot_client.stop()

    def run(self):
        self._iot_client.run()

    def invoke_api_get(self, name: str) -> dict:
        """Invokes method of the corresponding API class that performs GET request to AWS and returns the response"""
        api = self._endpoints.get(name)
        if api is None:
            raise CloudApiNotFound
        return api.get()

    def invoke_api_post(self, name: str, obj: dict):
        """Invokes method of the corresponding API class that performs POST request to AWS"""
        api = self._endpoints.get(name)
        if api is None:
            raise CloudApiNotFound
        api.post(obj)

    def invoke_api_post_with_response(self, name: str, obj: dict) -> dict:
        """Invokes method of the corresponding API class that performs POST request to AWS and returns response"""
        api = self._endpoints.get(name)
        if api is None:
            raise CloudApiNotFound
        return api.post(obj, response_back=True)

    def download_image(self, url: str, save_to: Path) -> str:
        """Downloads an image from the cloud datastore (S3) using HTTP GET request. Returns name of the file"""
        name = get_name_from_url(url)
        if name is None:
            self._logger.error(f"Unable to extract name from the url ({url})")
            raise CloudApiImageDownloadError("Failed to get image file name from URL")
        image_fname = save_to.joinpath(name)
        try:
            r = requests.get(url, timeout=AwsApi.HTTP_TIMEOUT_SECS)
        except requests.exceptions.ConnectionError as e:
            raise CloudApiConnectionError(str(e))
        except requests.exceptions.Timeout:
            raise CloudApiTimeoutError
        else:
            if r.status_code == requests.codes.ok:
                with open(image_fname, 'wb') as fb:
                    fb.write(r.content)
                self._logger.debug(f"Image file {name} is downloaded and stored")
                return name
            else:
                raise CloudApiServerError(r.status_code, r.text)

    def _event_handler(self, ev: Event):
        if ev.type == EventType.SEND_TO_CLOUD:
            try:
                self.invoke_api_post(ev.body['api'], ev.body['data'])
            except CloudApiNotFound:
                self._logger.error(f"API {ev.body['api']} is not configured")
            except CloudApiServerError as e:
                self._logger.error(f"Server returned status code {e.status_code} with message({e.response}) for "
                                   f"API {ev.body['api']}")
            except CloudApiConnectionError as e:
                self._logger.error(f"Unable to connect to the server, error - ({e.msg}) for API {ev.body['api']}")
            except CloudApiTimeoutError:
                self._logger.error(f"Timeout error occurred while trying to use API {ev.body['api']}")

