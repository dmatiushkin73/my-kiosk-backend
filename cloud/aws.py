from cloud.cloud_client import CloudClient
from core.logger import Logger
from cloud.mqtt_client import MqttClient
from cloud.aws_api import AwsApi
from core import utils
from core.event_bus import EventBus
from pathlib import Path
import requests


class AwsClient(CloudClient):
    """Implements access to AWS cloud"""
    MYNAME = 'cloud.api'
    REQ_CFG_OPTIONS = ['deviceId', 'customerId', 'iot', 'api_endpoints']

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus):
        super().__init__(AwsClient.MYNAME, config_data, logger, ev_bus)
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
        for endpoint in self._config['api_endpoints']:
            if utils.DEVICE_ID_PLACEHOLDER in endpoint['value']:
                endpoint['value'] = endpoint['value'].replace(utils.DEVICE_ID_PLACEHOLDER, self._device_id)
            if utils.CUSTOMER_ID_PLACEHOLDER in endpoint['value']:
                endpoint['value'] = endpoint['value'].replace(utils.CUSTOMER_ID_PLACEHOLDER, self._customer_id)
            self._endpoints[endpoint['name']] = AwsApi(endpoint['key'], endpoint['value'])
        super().start()
        self._logger.info("AWS Cloud client started")

    def stop(self):
        super().stop()
        self._logger.info("AWS Cloud client stopped")

    def _fill_common_fields(self, d: dict):
        """Checks if the given dictionary contains common keys like deviceId and sets their values"""
        for key in d:
            if key == 'deviceId':
                d[key] = self._device_id

    def invoke_api_get(self, name: str, params: dict) -> dict:
        """Invokes method of the corresponding API class that performs GET request to AWS and returns the response"""
        api = self._endpoints.get(name)
        if api is None:
            raise utils.CloudApiNotFound
        self._fill_common_fields(params)
        return api.get(params)

    def invoke_api_post(self, name: str, obj: dict):
        """Invokes method of the corresponding API class that performs POST request to AWS"""
        api = self._endpoints.get(name)
        if api is None:
            raise utils.CloudApiNotFound
        self._fill_common_fields(obj)
        api.post(obj)

    def invoke_api_post_with_response(self, name: str, obj: dict) -> dict:
        """Invokes method of the corresponding API class that performs POST request to AWS and returns response"""
        api = self._endpoints.get(name)
        if api is None:
            raise utils.CloudApiNotFound
        self._fill_common_fields(obj)
        return api.post(obj, response_back=True)

    def download_image(self, url: str, save_to: Path) -> str:
        """Downloads an image from the cloud datastore (S3) using HTTP GET request. Returns name of the file"""
        name = utils.get_name_from_url(url)
        if name is None:
            self._logger.error(f"Unable to extract name from the url ({url})")
            raise utils.CloudApiImageDownloadError("Failed to get image file name from URL")
        image_fname = save_to.joinpath(name)
        try:
            r = requests.get(url, timeout=AwsApi.HTTP_TIMEOUT_SECS)
        except requests.exceptions.ConnectionError as e:
            raise utils.CloudApiConnectionError(str(e))
        except requests.exceptions.Timeout:
            raise utils.CloudApiTimeoutError
        else:
            if r.status_code == requests.codes.ok:
                with open(image_fname, 'wb') as fb:
                    fb.write(r.content)
                self._logger.debug(f"Image file {name} is downloaded and stored")
                return name
            else:
                raise utils.CloudApiServerError(r.status_code, r.text)
