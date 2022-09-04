import hashlib
import time
import base64
import hmac
import json


class GopaxAuth():
    """
    Auth class required by Gopax API
    """

    def __init__(self, api_key: str, secret_key: str):
        self._api_key: str = api_key
        self._secret_key: str = secret_key

    def get_auth_headers(self, need_auth, method, path, body_json=None, recv_window=None):
        method = method.upper()

        if need_auth:
            timestamp = str(int(time.time() * 1000))
            include_querystring = method == 'GET' and path.startswith('/orders?')
            p = path if include_querystring else path.split('?')[0]
            msg = 't' + timestamp + method + p
            msg += (str(recv_window) if recv_window else '') + (json.dumps(body_json) if body_json else '')
            raw_secret = base64.b64decode(self._secret_key)
            raw_signature = hmac.new(raw_secret, str(msg).encode('utf-8'), hashlib.sha512).digest()
            signature = base64.b64encode(raw_signature)
            headers = {'api-key': self._api_key, 'timestamp': timestamp, 'signature': signature.decode('utf-8')}
            if recv_window:
                headers['receive-window'] = str(recv_window)
        else:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }

        return headers

    # def get_headers(self) -> Dict[str, Any]:
    #     """
    #     Generates authentication headers required by ProBit
    #     :return: a dictionary of auth headers
    #     """

    #     return {
    #         "Accept": "application/json",
    #         "Content-Type": "application/json"
    #     }

    # def get_auth_headers(self, query: str = None):
    #     headers = self.get_headers()
    #     headers.update(self.generate_auth_dict(need_auth, method, path, body_json, recv))
    #     return headers

    def get_ws_querystring(self):
        timestamp = str(int(time.time() * 1000))
        msg = 't' + timestamp
        key = base64.b64decode(self._secret_key)
        signature = base64.b64encode(
            hmac.new(key, str(msg).encode('utf-8'), hashlib.sha512).digest()
        ).decode()

        return f"?apiKey={self._api_key}&timestamp={timestamp}&signature={signature}"
