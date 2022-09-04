import aiohttp
import asyncio
import logging
from enum import Enum
from typing import AsyncIterable, Dict, Any, Optional, List

import ujson

import hummingbot.connector.exchange.bithumb.bithumb_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.connector.exchange.bithumb.bithumb_utils import convert_to_exchange_trading_pair, convert_from_exchange_trading_pair
from hummingbot.logger import HummingbotLogger

class BithumbWebSocketAdaptor:

    HEARTBEAT_INTERVAL = 30.0
    ONE_SEC_DELAY = 1.0

    DIFF_CHANNEL_ID = "orderbookdepth"
    TRADE_CHANNEL_ID = "transaction"
    SUBSCRIPTION_LIST = set([DIFF_CHANNEL_ID, TRADE_CHANNEL_ID])

    _TYPE_FIELD_NAME = "type"
    _CODES_FIELD_NAME = "symbols"

    _msg_type_field_name = "type"
    
    _logger: Optional[HummingbotLogger] = None

    """
    Auxiliary class that works as a wrapper of a low level web socket. It contains the logic to create messages
    with the format expected by NDAX
    :param websocket: The low level socket to be used to send and receive messages
    :param previous_messages_number: number of messages already sent to NDAX. This parameter is useful when the
    connection is reestablished after a communication error, and allows to keep a unique identifier for each message.
    The default previous_messages_number is 0
    """
    MESSAGE_TIMEOUT = 20.0
    PING_TIMEOUT = 5.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        shared_client: Optional[aiohttp.ClientSession] = None
    ):
        self._WS_URL = CONSTANTS.WSS_URLS["bithumb_main"]
        self._shared_client = shared_client
        self._websocket: Optional[aiohttp.ClientWebSocketResponse] = None

    def get_shared_client(self) -> aiohttp.ClientSession:
        if not self._shared_client:
            self._shared_client = aiohttp.ClientSession(auto_decompress=True)
        return self._shared_client

    async def _sleep(self, delay: float = 1.0):
        await asyncio.sleep(delay)

    async def send_request(self, payload: Dict[str, Any]):
        await self._websocket.send_json(payload)

    async def subscribe_to_order_book_streams(self, trading_pairs: List[str]):
        try:
            pairs = [convert_to_exchange_trading_pair(pair) for pair in trading_pairs]
            await self.send_request({self._TYPE_FIELD_NAME: self.TRADE_CHANNEL_ID, self._CODES_FIELD_NAME: pairs})
            await self.send_request({self._TYPE_FIELD_NAME: self.DIFF_CHANNEL_ID, self._CODES_FIELD_NAME: pairs})

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    async def connect(self):
        try:
            self._websocket = await self.get_shared_client().ws_connect(
                url=self._WS_URL, heartbeat=self.HEARTBEAT_INTERVAL
            )
            self.logger().info("Successfully connected to orderbook streams...")

        except Exception as e:
            self.logger().error(f"Websocket error: '{str(e)}'", exc_info=True)
            raise

    async def close(self):
        if self._websocket is not None:
            await self._websocket.close()

    async def iter_messages(self) -> AsyncIterable[Any]:
        try:
            while True:
                rsp = await self._websocket.receive()
                rsp = ujson.loads(rsp.data)
                yield rsp
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(f"Unexpected error occured when parsing websocket payload. "
                                  f"Error: {e}")
            raise
        finally:
            await self.close()


    @classmethod
    def decode_to_json(cls, msg):
        msg = ujson.loads(msg.decode('utf8'))
        return msg


    @classmethod
    def endpoint_from_raw_message(cls, raw_message: str) -> str:
        message = ujson.loads(raw_message)
        return cls.endpoint_from_message(message=message)

    @classmethod
    def endpoint_from_message(cls, message: Dict[str, Any]) -> str:
        return message.get(cls._endpoint_field_name)

    @classmethod
    def payload_from_raw_message(cls, raw_message: str) -> Dict[str, Any]:
        message = ujson.loads(raw_message)
        return cls.payload_from_message(message=message)

    @classmethod
    def payload_from_message(cls, message: Dict[str, Any]) -> Dict[str, Any]:
        payload = ujson.loads(message.get(cls._payload_field_name))
        return payload