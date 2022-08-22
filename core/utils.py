import hashlib

DEVICE_ID_PLACEHOLDER = '$deviceId'
CUSTOMER_ID_PLACEHOLDER = '$customerId'


def make_hash(s: str) -> bytes:
    return hashlib.sha224(bytes(s, 'utf-8')).digest()


def check_config(config: dict, req_options: list, key: str = None) -> (bool, str):
    """Checks that all required options in the given list req_options are present in the given config dictionary.
       If key is given, then check is done assuming config[key] is another dictionary
       that must have the given options
    """
    dict2check = config
    if key and len(key) > 0:
        dict2check = config[key]
    for opt in req_options:
        if opt not in dict2check:
            return False, opt
        if ((type(dict2check[opt]) == str or type(dict2check[opt]) == list or type(dict2check[opt]) == dict) and
                len(dict2check[opt]) == 0):
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


class DbBroken(Exception):
    pass


class DbError(Exception):
    def __init__(self, func: str, msg: str, dberror: str):
        self.funcname = func
        self.message = msg
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
