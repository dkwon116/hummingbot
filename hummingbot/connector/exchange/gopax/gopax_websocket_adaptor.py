import aiohttp
import asyncio
import logging
from typing import AsyncIterable, Dict, Any, Optional, List

import ujson
import json

import hummingbot.connector.exchange.gopax.gopax_constants as CONSTANTS
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.gopax.gopax_auth import GopaxAuth


class GopaxWebSocketAdaptor:

    HEARTBEAT_INTERVAL = 30.0
    ONE_SEC_DELAY = 1.0

    # DIFF_CHANNEL_ID = "SubscribeToOrderBook"
    ORDERBOOK_CHANNEL_ID = "SubscribeToTradingPair"

    ORDERBOOK_EVENT_NAME = "OrderBookEvent"
    TRADE_EVENT_NAME = "PublicTradeEvent"
    SUBSCRIPTION_LIST = set([ORDERBOOK_EVENT_NAME, TRADE_EVENT_NAME])
    
    ACCOUNT_POSITION_EVENT_NAME = "BalanceEvent"
    ORDER_STATE_EVENT_NAME = "OrderEvent"
    ORDER_TRADE_EVENT_NAME = "TradeEvent"
    USER_STREAM_EVENTS = set([ACCOUNT_POSITION_EVENT_NAME, ORDER_STATE_EVENT_NAME, ORDER_TRADE_EVENT_NAME])

    ORDER_CHANNEL_ID = "SubscribeToOrders"
    BALANCE_CHANNEL_ID = "SubscribeToBalances"
    TRADE_CHANNEL_ID = "SubscribeToTrades"

    _EVENT_FIELD_NAME = "n"
    _PARAM_FIELD_NAME = "o"

    _msg_type_field_name = "n"
    _msg_content_field_name = "o"
    _msg_stream_field_name = "i"
    _delta_msg = -1

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
        shared_client: Optional[aiohttp.ClientSession] = None,
        auth_assistant: Optional[GopaxAuth] = None
    ):
        self._WS_URL = CONSTANTS.WSS_URLS["gopax_main"]
        self._shared_client = shared_client
        self._websocket: Optional[aiohttp.ClientWebSocketResponse] = None
        self._auth_assistant = auth_assistant

    def get_shared_client(self) -> aiohttp.ClientSession:
        if not self._shared_client:
            self._shared_client = aiohttp.ClientSession(auto_decompress=True)
        return self._shared_client

    async def _sleep(self, delay: float = 1.0):
        await asyncio.sleep(delay)

    async def send_request(self, payload: Dict[str, Any]):
        await self._websocket.send_str(json.dumps(payload))

    async def subscribe_to_order_book_streams(self, trading_pairs: List[str]):
        try:
            for pair in trading_pairs:
                orderbook_sub_payload = {
                    self._EVENT_FIELD_NAME: self.ORDERBOOK_CHANNEL_ID,
                    self._PARAM_FIELD_NAME: {"tradingPairName": pair},
                }
                await self.send_request(orderbook_sub_payload)

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    async def subscribe_to_user_streams(self):
        try:
            balance_sub_payload = {
                self._EVENT_FIELD_NAME: self.BALANCE_CHANNEL_ID,
                self._PARAM_FIELD_NAME: {}
            }
            await self.send_request(balance_sub_payload)

            order_sub_payload = {
                self._EVENT_FIELD_NAME: self.ORDER_CHANNEL_ID,
                self._PARAM_FIELD_NAME: {}
            }
            await self.send_request(order_sub_payload)

            trade_sub_payload = {
                self._EVENT_FIELD_NAME: self.TRADE_CHANNEL_ID,
                self._PARAM_FIELD_NAME: {}
            }
            await self.send_request(trade_sub_payload)

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to user stream streams...", exc_info=True
            )
            raise

    async def connect(self):
        try:
            url = self._WS_URL + self._auth_assistant.get_ws_querystring()
            self._websocket = await self.get_shared_client().ws_connect(
                url=url, heartbeat=self.HEARTBEAT_INTERVAL
            )
            self.logger().info("Successfully connected to websocket streams...")

        except Exception as e:
            self.logger().error(f"Websocket error: '{str(e)}'", exc_info=True)
            raise

    async def close(self):
        if self._websocket is not None:
            await self._websocket.close()

    async def iter_messages(self) -> AsyncIterable[Any]:
        try:
            while True:
                rsp = self._websocket.receive()
                yield await rsp
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(f"Unexpected error occured when parsing websocket payload. "
                                  f"Error: {e}")
            raise
        finally:
            await self.close()

    async def _handle_ping_message(self, rsp):
        await self.send_request(CONSTANTS.WS_PONG + rsp[15:])

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
        return message.get(cls._msg_type_field_name)

    @classmethod
    def payload_from_raw_message(cls, raw_message: str) -> Dict[str, Any]:
        message = ujson.loads(raw_message)
        return cls.payload_from_message(message=message)

    @classmethod
    def payload_from_message(cls, message: Dict[str, Any]) -> Dict[str, Any]:
        payload = message.get(cls._msg_content_field_name)
        return payload

    @classmethod
    def convert_order_payload(cls, message: Dict[str, Any]) -> Dict[str, Any]:
        message["id"] = str(message["orderId"])
        if message["status"] == 2:
            message["status"] = "cancelled"
        elif message["status"] == 3:
            message["status"] = "completed"
        else:
            message["status"] = "placed"

        return message
    
    @classmethod
    def convert_trade_payload(cls, message: Dict[str, Any]) -> Dict[str, Any]:
        message["id"] = str(message["tradeId"])
        return message
