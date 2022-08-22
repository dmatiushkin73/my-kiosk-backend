from abc import ABC, abstractmethod
from core.logger import Logger
from core.utils import check_config, ConfigError


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

