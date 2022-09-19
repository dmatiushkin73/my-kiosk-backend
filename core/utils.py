import hashlib
import sys

DEVICE_ID_PLACEHOLDER = '$deviceId'
CUSTOMER_ID_PLACEHOLDER = '$customerId'


def make_hash(s: str) -> bytes:
    return hashlib.sha224(bytes(s, 'utf-8')).digest()


def check_config(config: dict, req_options: list) -> (bool, str):
    """Checks that all required options in the given list req_options are present in the given config dictionary.
       If an option has format A:B, then first it is checked that A is present in config and if yes,
       then it is assumed that config['A'] is also a dict and it is checked that B is present in it.
    """
    for opt in req_options:
        if ':' in opt:
            parts = opt.split(':')
            if parts[0] not in config:
                return False, parts[0]
            config_a = config[parts[0]]
            if type(config_a) != dict or len(config_a) == 0:
                return False, parts[0]
            if parts[1] not in config_a:
                return False, parts[1]
            config_b = config_a[parts[1]]
            if (type(config_b) == str or type(config_b) == list or type(config_b) == dict) and len(config_b) == 0:
                return False, parts[1]
        else:
            if opt not in config:
                return False, opt
            if ((type(config[opt]) == str or type(config[opt]) == list or type(config[opt]) == dict) and
                    len(config[opt]) == 0):
                return False, opt
    return True, None


def get_name_from_url(url: str) -> str | None:
    end_idx = url.rfind('?')
    if end_idx == -1:
        end_idx = len(url)
    start_idx = url.rfind('/')
    if start_idx != -1:
        return url[start_idx + 1: end_idx]
    return None


def _myname_(o: object) -> str:
    return str(o.__class__).split("'")[1] + "." + sys._getframe(1).f_code.co_name


class DbBroken(Exception):
    pass


class DbError(Exception):
    def __init__(self, func: str, msg: str, dberror: str):
        self.funcname = func
        self.msg = msg
        self.internal_error = dberror


class ConfigError(Exception):
    def __init__(self, module: str, failed_opt: str):
        self.module = module
        self.failed_option = failed_opt


class CloudApiFormatError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class CloudApiServerError(Exception):
    def __init__(self, status_code: int, response: str):
        self.status_code = status_code
        self.response = response


class CloudApiConnectionError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class CloudApiTimeoutError(Exception):
    pass


class CloudApiNotFound(Exception):
    pass


class CloudApiImageDownloadError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class UnsupportedFeatureError(Exception):
    def __init__(self, feature: str):
        self.feature = feature


class ModuleStartupError(Exception):
    def __init__(self, module: str, msg: str, err: str):
        self.module = module
        self.msg = msg
        self.error = err
