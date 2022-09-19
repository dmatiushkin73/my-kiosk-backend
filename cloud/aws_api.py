import json
import requests
from core.utils import CloudApiFormatError, CloudApiServerError, CloudApiConnectionError, CloudApiTimeoutError


class AwsApi:
    """Implements access to AWS HTTP APIs"""
    HTTP_TIMEOUT_SECS = 15

    def __init__(self, key: str, url: str):
        self._api_key = key
        self._api_url = url

    def get(self, params: dict) -> dict:
        headers = {}
        if len(self._api_key) > 0:
            headers["X-Api-Key"] = self._api_key
        url = self._api_url
        for k, v in params.items():
            url += f"&{k}=v"
        try:
            r = requests.get(url, headers=headers, timeout=AwsApi.HTTP_TIMEOUT_SECS)
            if r.status_code == requests.codes.ok:
                try:
                    obj = r.json()
                    return obj
                except ValueError as e:
                    raise CloudApiFormatError(str(e))
            else:
                raise CloudApiServerError(r.status_code, r.text)
        except requests.exceptions.ConnectionError as e:
            raise CloudApiConnectionError(str(e))
        except requests.exceptions.Timeout:
            raise CloudApiTimeoutError

    def post(self, obj: dict, response_back: bool = False) -> dict | None:
        data = json.dumps(obj)
        headers = {}
        if len(self._api_url) > 0:
            headers["X-Api-Key"] = self._api_key
        headers['Content-Type'] = 'application/json'
        headers['Content-Length'] = str(len(data))
        try:
            r = requests.post(self._api_url, headers=headers, timeout=AwsApi.HTTP_TIMEOUT_SECS)
            if r.status_code == requests.codes.ok:
                if response_back:
                    try:
                        obj = r.json()
                        return obj
                    except ValueError as e:
                        raise CloudApiFormatError(str(e))
                else:
                    return None
            else:
                raise CloudApiServerError(r.status_code, r.text)
        except requests.exceptions.ConnectionError as e:
            raise CloudApiConnectionError(str(e))
        except requests.exceptions.Timeout:
            raise CloudApiTimeoutError
