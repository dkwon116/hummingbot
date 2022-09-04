import hashlib
import hmac
import jwt
import uuid

from typing import Dict, Any

from hummingbot.connector.exchange.upbit import upbit_utils



class UpbitAuth():
    """
    Auth class required by Upbit API
    """

    def __init__(self, api_key: str, secret_key: str):
        self._api_key: str = api_key
        self._secret_key: str = secret_key

    def generate_auth_dict(self, params: str = None) -> Dict[str, Any]:
        """
        Generates a dictionary with all required information for the authentication process
        :return: a dictionary of authentication info including the request signature
        """

        payload = {
            'access_key': self._api_key,
            'nonce': str(uuid.uuid4())
        }

        if params:
            # print(f"params to urlencode {params}")
            query = upbit_utils.generate_query(params)
            # print(f"generated url query {query}")
            m = hashlib.sha512()
            m.update(query.encode())
            query_hash = m.hexdigest()
            payload['query_hash'] = query_hash
            payload['query_hash_alg'] = 'SHA512'

        jwt_token = jwt.encode(payload, self._secret_key)
        decoded_jwt = jwt_token.decode("utf-8")
        auth_token = f'Bearer {decoded_jwt}'

        return {"Authorization": auth_token}

    def get_headers(self) -> Dict[str, Any]:
        """
        Generates authentication headers required by ProBit
        :return: a dictionary of auth headers
        """

        return {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def get_auth_headers(self, query: str = None):
        headers = self.get_headers()
        headers.update(self.generate_auth_dict(query))
        return headers
