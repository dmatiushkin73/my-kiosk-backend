import json
import sys
from pathlib import Path
from core.logger import Logger
from core import utils
from core.event_bus import EventBus
from db.database import Database
from db.model import AccessLevel
from cloud.cloud_client import CloudClient
from cloud.aws import AwsClient
from logic.planogram import PlanogramLogic
from logic.cart import CartLogic
from logic.machine import MachineLogic
from ui.backend_server import BackendRestServer
from ui.ws_server import BackendWebsocketServer


class KioskBackend:
    """Main Kiosk Backend application class"""
    CFG_FILE = 'config.json'
    REQ_CFG_OPTIONS = ['general', 'database', 'cloud', 'cloud:type', 'hardware', 'communication', 'telemetry',
                       'ui', 'ui:rest_server', 'ui:websocket_server',
                       'logic', 'logic:planogram', 'logic:cart', 'logic:login', 'logger']
    DEFAULT_LANGUAGE = "en"

    def __init__(self):
        try:
            f = open(KioskBackend.CFG_FILE)
            self._config = json.loads(f.read())
        except Exception as e:
            print(f"Critical error: failed to open or read {KioskBackend.CFG_FILE} - {str(e)}")
            raise RuntimeError("Configuration is unavailable")
        self._validate_config()
        self._lang = self._config['general'].get('language', KioskBackend.DEFAULT_LANGUAGE)
        self._cwd = Path.cwd()
        self._data_dir = self._cwd.joinpath('data')
        self._img_dir = self._data_dir.joinpath('images')
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._img_dir.mkdir(parents=True, exist_ok=True)
        self._logger = Logger(self._config['logger'])
        self._event_bus = EventBus({}, self._logger)
        self._database = Database(self._config['database'], self._logger, self._data_dir, self._cwd.joinpath('db'),
                                  self._lang)
        self._cloud_client: CloudClient = None
        self._planogram_logic: PlanogramLogic = None
        self._cart_logic: CartLogic = None
        self._ui_backend: BackendRestServer = None
        self._ui_ws: BackendWebsocketServer = None
        self._machine_logic: MachineLogic = None

    def start(self, args: list) -> bool:
        """Returns False if application should exit immediately"""
        self._logger.info('='*80)
        self._logger.info("JER Kiosk Backend application is starting")
        self._event_bus.validate_config()
        self._database.validate_config()
        self._event_bus.start()
        self._database.start()
        if len(args) == 4 and args[1] == '--adduser':
            try:
                self._database.add_user(args[2], utils.make_hash(args[3]), AccessLevel.ADMIN)
                self._logger.info("New user is added, exiting")
            except utils.DbError:
                pass
            return False
        cloud_type = self._config['cloud']['type']
        if cloud_type not in self._config['cloud']:
            raise utils.ConfigError('cloud', cloud_type)
        if cloud_type == 'aws':
            self._cloud_client = AwsClient(self._config['cloud']['aws'], self._logger, self._event_bus)
        else:
            raise utils.UnsupportedFeatureError(f"Cloud type '{cloud_type}'")
        self._cloud_client.validate_config()
        self._planogram_logic = PlanogramLogic(self._config['logic']['planogram'], self._logger, self._event_bus,
                                               self._cloud_client, self._database, self._data_dir, self._img_dir)
        self._planogram_logic.validate_config()
        self._cart_logic = CartLogic(self._config['logic']['cart'], self._logger, self._event_bus,
                                     self._cloud_client, self._database)
        self._cart_logic.validate_config()
        self._ui_backend = BackendRestServer(self._config['ui']['rest_server'], self._logger, self._event_bus,
                                             self._database, self._cart_logic, self._data_dir, self._lang)
        self._ui_backend.validate_config()
        self._ui_ws = BackendWebsocketServer(self._config['ui']['websocket_server'], self._logger, self._event_bus,
                                             self._database)
        self._machine_logic = MachineLogic({}, self._logger, self._event_bus, self._planogram_logic)
        self._machine_logic.validate_config()
        self._ui_ws.validate_config()
        self._cloud_client.start()
        self._planogram_logic.start()
        self._cart_logic.start()
        self._ui_backend.start()
        self._ui_ws.start()
        self._machine_logic.start()
        self._logger.info("JER Kiosk Backend application started")
        return True

    def cleanup(self):
        self._logger.info("JER Kiosk Backend application is stopping")
        self._machine_logic.stop()
        self._ui_ws.stop()
        self._ui_backend.stop()
        self._cart_logic.stop()
        self._planogram_logic.stop()
        self._event_bus.stop()
        if self._cloud_client:
            self._cloud_client.stop()
        self._database.stop()
        self._logger.info("JER Kiosk Backend application stopped")

    def run(self):
        if self._cloud_client:
            self._cloud_client.run()

    def _validate_config(self):
        res, opt = utils.check_config(self._config, KioskBackend.REQ_CFG_OPTIONS)
        if not res:
            raise utils.ConfigError('app', opt)


if __name__ == '__main__':
    try:
        app = KioskBackend()
        shall_run = app.start(sys.argv)
    except utils.ConfigError as e:
        print(f"Critical error in configuration file, option {e.failed_option} in module {e.module} is absent or empty")
        raise RuntimeError("Application is unable to start")
    except utils.DbBroken as e:
        print(f"Critical error occurred during database initialization - {str(e)}")
        raise RuntimeError("Application is unable to start")
    except utils.UnsupportedFeatureError as e:
        print(f"Critical error during application startup, feature {e.feature} is not supported")
        raise RuntimeError("Application is unable to start")
    if not shall_run:
        app.cleanup()
    else:
        try:
            app.run()
        except KeyboardInterrupt:
            pass
        finally:
            app.cleanup()
