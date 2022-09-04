import aiohttp
import asyncio
import logging
import time
import ujson

from typing import (Optional)

from hummingbot.connector.exchange.gopax.gopax_auth import GopaxAuth
from hummingbot.connector.exchange.gopax import gopax_constants as CONSTANTS
from hummingbot.connector.exchange.gopax.gopax_websocket_adaptor import GopaxWebSocketAdaptor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.utils.async_utils import safe_ensure_future


class GopaxAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, throttler: AsyncThrottler, auth_assistant: GopaxAuth, shared_client: Optional[aiohttp.ClientSession] = None, domain: Optional[str] = None):
        super().__init__()
        self._shared_client = shared_client or self._get_session_instance()
        self._ws_adaptor = None
        self._auth_assistant: GopaxAuth = auth_assistant
        self._last_recv_time: float = 0
        self._domain = domain
        self._throttler = throttler

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    @classmethod
    def _get_session_instance(cls) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession()
        return session

    async def _init_websocket_connection(self) -> GopaxWebSocketAdaptor:
        """
        Initialize WebSocket client for UserStreamDataSource
        """
        try:
            if self._ws_adaptor is None:
                ws = GopaxWebSocketAdaptor(shared_client=self._shared_client, auth_assistant=self._auth_assistant)
                self._ws_adaptor = await ws.connect()
            return ws
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            self.logger().network(f"Unexpected error occurred during {CONSTANTS.EXCHANGE_NAME} WebSocket Connection "
                                  f"({ex})")
            raise

    # async def _authenticate(self, ws: GopaxWebSocketAdaptor):
    #     """
    #     Authenticates user to websocket
    #     """
    #     try:
    #         auth_payload: Dict[str, Any] = self._auth_assistant.get_ws_auth_payload()
    #         async with self._throttler.execute_task(CONSTANTS.AUTHENTICATE_USER_ENDPOINT_NAME):
    #             await ws.send_request(CONSTANTS.AUTHENTICATE_USER_ENDPOINT_NAME, auth_payload)
    #         auth_resp = await ws.receive()
    #         auth_payload: Dict[str, Any] = ws.payload_from_raw_message(auth_resp.data)

    #         if not auth_payload["Authenticated"]:
    #             self.logger().error(f"Response: {auth_payload}",
    #                                 exc_info=True)
    #             raise Exception("Could not authenticate websocket connection with NDAX")

    #         auth_user = auth_payload.get("User")
    #         self._account_id = auth_user.get("AccountId")
    #         self._oms_id = auth_user.get("OMSId")

    #     except asyncio.CancelledError:
    #         raise
    #     except Exception as ex:
    #         self.logger().error(f"Error occurred when authenticating to user stream ({ex})",
    #                             exc_info=True)
    #         raise

    # async def _subscribe_to_events(self, ws: GopaxWebSocketAdaptor):
    #     """
    #     Subscribes to User Account Events
    #     """
    #     payload = {"AccountId": self._account_id,
    #                "OMSId": self._oms_id}
    #     try:
    #         async with self._throttler.execute_task(CONSTANTS.SUBSCRIBE_ACCOUNT_EVENTS_ENDPOINT_NAME):
    #             await ws.send_request(CONSTANTS.SUBSCRIBE_ACCOUNT_EVENTS_ENDPOINT_NAME, payload)

    #     except asyncio.CancelledError:
    #         raise
    #     except Exception as ex:
    #         self.logger().error(f"Error occurred subscribing to {CONSTANTS.EXCHANGE_NAME} private channels ({ex})",
    #                             exc_info=True)
    #         raise

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        *required
        Subscribe to user stream via web socket, and keep the connection open for incoming messages
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        """
        while True:
            try:
                ws = await self._init_websocket_connection()
                # self.logger().info("Authenticating to User Stream...")
                # await self._authenticate(ws)
                self.logger().info("Successfully authenticated to User Stream.")
                await ws.subscribe_to_user_streams()
                self.logger().info("Successfully subscribed to user events.")

                async for msg in ws.iter_messages():
                    msg = ujson.loads(msg.data)

                    if isinstance(msg, (str)):
                        if msg.startswith(CONSTANTS.WS_PING):
                            safe_ensure_future(ws._handle_ping_message(msg))
                            continue
                    if "errCode" in msg[GopaxWebSocketAdaptor._msg_content_field_name]:
                        raise Exception()

                    self._last_recv_time = int(time.time())    
                    if GopaxWebSocketAdaptor._msg_type_field_name not in msg or GopaxWebSocketAdaptor._msg_stream_field_name not in msg:
                        continue
                    channel = msg[GopaxWebSocketAdaptor._msg_type_field_name]
                    if channel in GopaxWebSocketAdaptor.USER_STREAM_EVENTS:
                        output.put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self.logger().error(
                    f"Unexpected error with Gopax User Stream WS connection. Retrying in 5 seconds. ({ex})",
                    exc_info=True
                )
                if self._ws_adaptor is not None:
                    await self._ws_adaptor.close()
                    self._ws_adaptor = None
                await asyncio.sleep(5.0)
