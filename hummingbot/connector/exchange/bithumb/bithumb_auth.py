import hashlib
import hmac
import uuid
import urllib
import base64
import time

from typing import Dict, Any

from hummingbot.connector.exchange.bithumb import bithumb_utils



class BithumbAuth():
    """
    Auth class required by Bithumb API
    """

    def __init__(self, api_key: str, secret_key: str):
        self._api_key: str = api_key
        self._secret_key: str = secret_key.encode('utf-8')
    
    def _signature(self, path, nonce, params):
        query_string = path + chr(0) + urllib.parse.urlencode(params) + chr(0) + nonce
        h = hmac.new(self._secret_key, query_string.encode('utf-8'), hashlib.sha512)
        return base64.b64encode(h.hexdigest().encode('utf-8'))

    def get_auth_headers(self, path, params):
        params['endpoint'] = path
        nonce = str(int(time.time() * 1000))
        signature = self._signature(path, nonce, params)

        return {
            'Api-Key': self._api_key,
            'Api-Sign': signature.decode('utf-8'),
            'Api-Nonce': nonce
        }