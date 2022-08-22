import paho.mqtt.client as mqtt
import time
from cloud.iot_client import IotClient
from core.logger import Logger
from core.utils import DEVICE_ID_PLACEHOLDER, CUSTOMER_ID_PLACEHOLDER


class MqttClient(IotClient):
    """Uses MQTT library to implement connection logic to the cloud."""
    REQ_CFG_OPTIONS = ['endpoint', 'ca_certificate', 'certificate', 'private_key', 'topics']
    DEFAULT_PORT = 8883
    DEFAULT_MAX_MSG_SIZE = 4096
    DEFAULT_KEEP_ALIVE_INTERVAL = 60

    def __init__(self, config_data: dict, logger: Logger):
        super().__init__(config_data, logger)
        self._device_id = ''
        self._customer_id = ''
        self._mqtt_client = None

    def _get_my_required_cfg_options(self) -> list:
        return MqttClient.REQ_CFG_OPTIONS

    def set_params(self, device_id: str, customer_id: str):
        self._device_id = device_id
        self._customer_id = customer_id
        for topic in self._config['topics']:
            if DEVICE_ID_PLACEHOLDER in topic['value']:
                topic['value'] = topic['value'].replace(DEVICE_ID_PLACEHOLDER, self._device_id)
            if CUSTOMER_ID_PLACEHOLDER in topic['value']:
                topic['value'] = topic['value'].replace(CUSTOMER_ID_PLACEHOLDER, self._customer_id)

    def start(self):
        self._mqtt_client = mqtt.Client(client_id=self._device_id, userdata=self)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_disconnect = self.on_disconnect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.on_log = self.on_log
        self.connect()

    def stop(self):
        self.disconnect()

    def connect(self):
        """Initiates connection with MQTT server"""
        if 'sas_token' in self._config and len(self._config['sas_token']) > 0:
            username = f"{self._config['endpoint']} / {self._device_id}"
            self._mqtt_client.username_pw_set(username, self._config['sas_token'])

        try:
            self._mqtt_client.tls_set(self._config['ca_certificate'],
                                      self._config['certificate'],
                                      self._config['private_key'])
        except FileNotFoundError:
            self._logger.critical("Mandatory certificate or private key file is not found")
            raise RuntimeError("Cannot establish connection via MQTT")

        if 'port' not in self._config:
            self._config['port'] = MqttClient.DEFAULT_PORT
        if 'max_message_size' not in self._config:
            self._config['max_message_size'] = MqttClient.DEFAULT_MAX_MSG_SIZE
        if 'keep_alive_interval' not in self._config:
            self._config['keep_alive_interval'] = MqttClient.DEFAULT_KEEP_ALIVE_INTERVAL

        for i in range(IotClient.CONNECT_ATTEMPTS):
            try:
                self._mqtt_client.connect(self._config['endpoint'],
                                          self._config['port'],
                                          self._config['keep_alive_interval'])
            except Exception as e:
                self._logger.warning(f"Exception caught calling MQTT connect: {str(e)}")
                time.sleep(IotClient.CONNECT_TIMEOUT)
            finally:
                return
        self._logger.critical("Failed to establish connection via MQTT")
        raise RuntimeError("Cannot establish connection via MQTT")

    def disconnect(self):
        self._mqtt_client.disconnect()

    def run(self):
        """Invokes MQTT client loop to process network events."""
        self._mqtt_client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        """Handles MQTT on_connect event. Subscribes to the topics"""
        self._logger.info("Connected with result: "+mqtt.error_string(rc))
        if rc == mqtt.MQTT_ERR_SUCCESS:
            for topic in self._config['topics']:
                result, mid = self._mqtt_client.subscribe(topic, 1)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    self._logger.info(f"Successfully subscribed to topic {topic}, mid={mid}")
                else:
                    err_msg = f"Failed to subscribe to topic {topic} - "
                    err_msg = err_msg + mqtt.error_string(result)
                    self._logger.error(err_msg)
                    # TODO: What to do in this case?

    def on_disconnect(self, client, userdata, rc):
        """Handles MQTT on_disconnect event"""
        self._logger.warning("Disconnected with result: "+mqtt.error_string(rc))
       
    def on_message(self, client, userdata, msg):
        """Handles MQTT on_message event"""
        self._logger.debug(f"Received message {msg.topic} : {str(msg.payload)}")
        if msg.topic in self._handlers:
            try:
                self._handlers[msg.topic](msg.payload.decode())
            except Exception as e:
                self._logger.error(f"Caught exception in handler for topic {msg.topic}: {str(e)}")
        else:
            self._logger.warning(f"No handler found for topic {msg.topic}")

    def on_log(self, client, userdata, level, buf):
        if level == mqtt.MQTT_LOG_INFO or level == mqtt.MQTT_LOG_NOTICE:
            self._logger.info(f"MQTT: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            self._logger.warning(f"MQTT: {buf}")
        elif level == mqtt.MQTT_LOG_ERR:
            self._logger.error(f"MQTT: {buf}")
        else:
            self._logger.debug(f"MQTT: {buf}")
