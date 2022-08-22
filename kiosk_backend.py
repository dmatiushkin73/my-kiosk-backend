import json
import sys
from pathlib import Path
from core.logger import Logger
from core import utils
from core.event_bus import EventBus
from db.database import Database
from db.model import AccessLevel
from cloud.aws import AwsClient


class KioskBackend:
    """Main Kiosk Backend application class"""
    CFG_FILE = 'config.json'
    REQ_CFG_OPTIONS = ['database', 'cloud', 'hardware', 'communication', 'telemetry', 'ui', 'logic', 'logger']

    def __init__(self):
        try:
            f = open(KioskBackend.CFG_FILE)
            self._config = json.loads(f.read())
        except Exception as e:
            print(f"Critical error: failed to open or read {KioskBackend.CFG_FILE} - {str(e)}")
            raise RuntimeError("Configuration is unavailable")
        self._validate_config()
        self._cwd = Path.cwd()
        self._data_dir = self._cwd.joinpath('data')
        self._img_dir = self._data_dir.joinpath('images')
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._img_dir.mkdir(parents=True, exist_ok=True)
        self._logger = Logger(self._config['logger'])
        self._event_bus = EventBus({}, self._logger)
        self._database = Database(self._config['database'], self._logger, self._data_dir, self._cwd.joinpath('db'))
        self._cloud_client = None

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
        cloud_type = self._config['cloud'].get('type')
        if cloud_type is None:
            raise utils.ConfigError('cloud', 'type')
        if cloud_type not in self._config['cloud']:
            raise utils.ConfigError('cloud', cloud_type)
        if cloud_type == 'aws':
            self._cloud_client = AwsClient(self._config['cloud']['aws'], self._logger, self._event_bus)
        else:
            raise utils.UnsupportedFeatureError(f"Cloud type '{cloud_type}'")
        self._cloud_client.validate_config()
        self._cloud_client.start()
        self._logger.info("JER Kiosk Backend application started")
        return True

    def cleanup(self):
        self._logger.info("JER Kiosk Backend application is stopping")
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
