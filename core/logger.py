import logging
import logging.handlers

LOG_FILENAME = 'kiosk-backend.log'
LOGGER_NAME = 'backend'
LOG_LEVELS = {"DEBUG": logging.DEBUG,
              "INFO": logging.INFO,
              "WARNING": logging.WARNING,
              "ERROR": logging.ERROR,
              "CRITICAL": logging.CRITICAL}


def get_log_level(lev: str) -> int:
    if lev in LOG_LEVELS:
        return LOG_LEVELS[lev]
    else:
        return logging.WARNING


class ModuleLogger:
    """A helper class that provides logging facility for a module"""
    def __init__(self, module: str, level: str):
        self._logger = logging.getLogger(LOGGER_NAME + '.' + module)
        self._level = get_log_level(level)

    def debug(self, msg: str):
        """Invokes debug method of module's logger if the configured module's level allows it"""
        if self._level <= logging.DEBUG:
            self._logger.debug(msg, stacklevel=2)

    def info(self, msg: str):
        """Invokes info method of module's logger if the configured module's level allows it"""
        if self._level <= logging.INFO:
            self._logger.info(msg, stacklevel=2)

    def warning(self, msg: str):
        """Invokes warning method of module's logger if the configured module's level allows it"""
        if self._level <= logging.WARNING:
            self._logger.warning(msg, stacklevel=2)

    def error(self, msg: str):
        """Invokes error method of module's logger if the configured module's level allows it"""
        if self._level <= logging.ERROR:
            self._logger.error(msg, stacklevel=2)

    def critical(self, msg: str):
        """Invokes critical method of module's logger"""
        self._logger.critical(msg, stacklevel=2)


class Logger:
    """Implements a multi module logging infrastructure over standard logging facility"""

    def __init__(self, config: dict):
        self._config = config
        self._logger = logging.getLogger(LOGGER_NAME)
        self._logger.setLevel(get_log_level(self._config["general_level"]))
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)-20s [%(funcName)s]: %(message)s')
        handler = logging.handlers.RotatingFileHandler(LOG_FILENAME,
                                                       maxBytes=self._config["max_file_size"]*1024*1024,  # param in MB
                                                       backupCount=self._config['max_backup_files'])
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._modules = dict()

    def get_logger(self, module: str) -> ModuleLogger:
        """Creates instance of the helper class, stores it and return"""
        level = ''
        for item in self._config['levels']:
            if item['module'] == module:
                level = item['level']
                break
        if level == '':
            level = 'WARNING'
            self._logger.warning(f"Module {module} requested logger but no level configured for it, will use WARNING")
        mod_logger = ModuleLogger(module, level)
        self._modules[module] = mod_logger
        return mod_logger

    def debug(self, msg: str):
        """Invokes debug method of main logger"""
        self._logger.debug(msg, stacklevel=2)

    def info(self, msg: str):
        """Invokes info method of main logger"""
        self._logger.info(msg, stacklevel=2)

    def warning(self, msg: str):
        """Invokes warning method of main logger"""
        self._logger.warning(msg, stacklevel=2)

    def error(self, msg: str):
        """Invokes error method of main logger"""
        self._logger.error(msg, stacklevel=2)

    def critical(self, msg: str):
        """Invokes critical method of main logger"""
        self._logger.critical(msg, stacklevel=2)
