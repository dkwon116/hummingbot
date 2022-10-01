# -*- coding: utf-8 -*-

import logging
import asyncio
from typing import Dict, Any, Optional
from time import monotonic
from datetime import datetime
import requests
import aiohttp

from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest
from hummingbot.connector.exchange.korbit import korbit_constants as CONSTANTS, korbit_web_utils as web_utils

s_logger = None

SAFE_TIME_PERIOD_SECONDS = 5
TOKEN_REFRESH_PERIOD_SECONDS = 10 * 60
MIN_TOKEN_LIFE_TIME_SECONDS = 30
TOKEN_OBTAIN_TIMEOUT = 30


class KorbitAuth(AuthBase):

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        # self._session_data_cache: Dict[str, Any] = {}

        self.token: Optional[str] = None
        self.token_obtain = asyncio.Event()
        self.token_obtain_started = False
        self.token_valid_to: float = 0
        self.token_next_refresh: float = 0
        self.token_obtain_start_time = 0
        self.token_raw_expires = 0
        self.refresh_token = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.

        :param request: the request to be configured for authenticated interaction

        :return: The RESTRequest with auth information included
        """

        headers = self.generate_auth_dict()
        if request.headers is not None:
            headers.update(request.headers)
        headers.update()
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. OKX does not use this
        functionality
        """
        return request  # pass-through

    def is_token_valid(self):
        return self.token_valid_to > monotonic()

    def token_timings_str(self):
        return f'auth req start time {self.token_obtain_start_time}, token validness sec {self.token_raw_expires}'

    def invalidate_token(self):
        self.token_valid_to = 0

    def update_with_new_token(self, new_token, new_refresh_token):
        self.token = new_token
        self.refresh_token = new_refresh_token

    def get_token(self):

        for _ in range(3):
            if self.is_token_valid():
                return self.token

            # token is invalid, waiting for a renew
            if not self.token_obtain_started:
                # if process of refreshing is not started, start it
                self.token, self.refresh_token = self._update_token()
                self.logger().info(f"Get Token after update {self.token} {self.refresh_token} Valid for {self.token_valid_to - monotonic()}")
                if not self.is_token_valid():
                    continue
                return self.token

            # waiting for fresh token
            # await asyncio.wait_for(self.token_obtain.wait(), timeout=TOKEN_OBTAIN_TIMEOUT)

            if not self.is_token_valid():
                continue
            return self.token

        raise ValueError('Invalid auth token timestamp')

    def _update_token(self):
        self.token_obtain_started = True

        try:

            start_time = monotonic()
            start_timestamp = datetime.now()

            data = {
                'client_id': self.api_key,
                'client_secret': self.secret_key,
                "grant_type": "client_credentials"
            }

            if self.refresh_token is not None:
                data = {
                    'client_id': self.api_key,
                    'client_secret': self.secret_key,
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token
                }

            response = requests.post(
                url=web_utils.public_rest_url(CONSTANTS.KORBIT_AUTH_PATH),
                data=data)
            self.logger().info(f"Token rsp {response} {response.status_code}")

            if response.status_code == 401:
                data = {
                    'client_id': self.api_key,
                    'client_secret': self.secret_key,
                    "grant_type": "client_credentials"
                }

                response = requests.post(
                    url=web_utils.public_rest_url(CONSTANTS.KORBIT_AUTH_PATH),
                    data=data)
                self.logger().info(f"Token rsp {response} {response.status_code}")

            if response.status_code != 200:
                raise IOError(f'Error while connecting to login token endpoint. HTTP status is {response.status_code}.')

            self.logger().info(f"Token json {response.json()}")

            token_rsp = response.json()
            self.logger().info(f"Token json {token_rsp}")

            if token_rsp['token_type'] != 'Bearer':
                raise IOError(f'Error while connecting to login token endpoint. Token type is {token_rsp["type"]}.')

            if int(token_rsp['expires_in']) < MIN_TOKEN_LIFE_TIME_SECONDS:
                raise IOError(f'Error while connecting to login token endpoint. Token lifetime to small {token_rsp["expires_in"]}.')

            self.update_with_new_token(token_rsp['access_token'], token_rsp['refresh_token'])
            self.token_raw_expires = token_rsp['expires_in']

            # include safe interval, e.g. time that approx network request can take
            self.token_obtain_start_time = start_timestamp
            self.token_valid_to = start_time + int(token_rsp['expires_in']) - SAFE_TIME_PERIOD_SECONDS
            self.token_next_refresh = start_time + TOKEN_REFRESH_PERIOD_SECONDS

            self.logger().info(f"Update Token {self.token} {self.refresh_token}")

            if not self.is_token_valid():
                raise ValueError('Invalid auth token timestamp')

        finally:
            self.token_obtain_started = False
            self.logger().info(f"Update Finally {self.token} {self.refresh_token}")
            return self.token, self.refresh_token

    # async def _update_token(self):

    #     self.token_obtain.clear()
    #     self.token_obtain_started = True

    #     try:

    #         start_time = monotonic()
    #         start_timestamp = datetime.now()

    #         async with aiohttp.ClientSession() as client:
    #             data = {
    #                 'client_id': self.api_key,
    #                 'client_secret': self.secret_key,
    #                 "grant_type": "client_credentials"
    #             }

    #             if self.refresh_token is not None:
    #                 data = {
    #                     'client_id': self.api_key,
    #                     'client_secret': self.secret_key,
    #                     "grant_type": "refresh_token",
    #                     "refresh_token": self.refresh_token
    #                 }

    #             async with client.post(
    #                     web_utils.public_rest_url(CONSTANTS.KORBIT_AUTH_PATH),
    #                     data=data
    #             ) as response:
    #                 response: aiohttp.ClientResponse = response
    #                 if response.status != 200:
    #                     raise IOError(f'Error while connecting to login token endpoint. HTTP status is {response.status}.')
    #                 data: Dict[str, str] = await response.json()

    #                 if data['token_type'] != 'Bearer':
    #                     raise IOError(f'Error while connecting to login token endpoint. Token type is {data["type"]}.')

    #                 if int(data['expires_in']) < MIN_TOKEN_LIFE_TIME_SECONDS:
    #                     raise IOError(f'Error while connecting to login token endpoint. Token lifetime to small {data["expires_in"]}.')

    #                 self.update_with_new_token(data['access_token'], data['refresh_token'])
    #                 self.token_raw_expires = data['expires_in']

    #                 # include safe interval, e.g. time that approx network request can take
    #                 self.token_obtain_start_time = start_timestamp
    #                 self.token_valid_to = start_time + int(data['expires_in']) - SAFE_TIME_PERIOD_SECONDS
    #                 self.token_next_refresh = start_time + TOKEN_REFRESH_PERIOD_SECONDS
                    
    #                 self.logger().info(f"Update Token {self.token} {self.refresh_token}")

    #                 if not self.is_token_valid():
    #                     raise ValueError('Invalid auth token timestamp')

    #                 self.token_obtain.set()

    #     finally:
    #         self.token_obtain_started = False
    #         self.logger().info(f"Update Finally {self.token} {self.refresh_token}")
    #         return self.token, self.refresh_token

    async def _auth_token_polling_loop(self):
        """
        Separate background process that periodically regenerates auth token
        """
        while True:
            try:
                # await safe_gather(self._update_token())
                self.token, self.refresh_token = self._update_token()
                await asyncio.sleep(TOKEN_REFRESH_PERIOD_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    'Unexpected error while fetching auth token.',
                    exc_info=True,
                    app_warning_msg='Could not fetch trading rule updates on Korbit. '
                                    'Check network connection.'
                )
                await asyncio.sleep(0.5)

    def generate_auth_dict(self) -> Dict[str, Any]:
        auth_token = self.get_token()
        return {'Authorization': f'Bearer {auth_token}'}
