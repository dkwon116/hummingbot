#!/usr/bin/env python
import aiohttp
import asyncio
import cachetools.func
import logging
import requests

import simplejson
import time
import ujson
import websockets

from decimal import Decimal
from signalr_aio import Connection
from typing import Optional, List, Dict, AsyncIterable, Any, TYPE_CHECKING
from websockets.exceptions import ConnectionClosed

from hummingbot.connector.derivative.ftx_perp.ftx_perp_order_book import FtxPerpOrderBook
from hummingbot.connector.derivative.ftx_perp.ftx_perp_utils import convert_from_exchange_trading_pair, convert_to_exchange_trading_pair
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger


import asyncio
from datetime import datetime
# from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.ftx_perp import ftx_perp_constants as CONSTANTS, ftx_perp_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
# from hummingbot.logger import HummingbotLogger
from hummingbot.connector.derivative.ftx_perp.ftx_perp_web_utils import build_api_factory

if TYPE_CHECKING:
    from hummingbot.connector.derivative.ftx_perp.ftx_perp_derivative import FtxPerpDerivative

EXCHANGE_NAME = "ftx_perp"

FTX_REST_URL = "https://ftx.com/api"
FTX_EXCHANGE_INFO_PATH = "/markets"
FTX_WS_FEED = "wss://ftx.com/ws/"

MAX_RETRIES = 20
SNAPSHOT_TIMEOUT = 10.0
NaN = float("nan")
API_CALL_TIMEOUT = 5.0


class FtxPerpAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _ftx_perpaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._ftx_perpaobds_logger is None:
            cls._ftx_perpaobds_logger = logging.getLogger(__name__)
        return cls._ftx_perpaobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None, api_factory: Optional[WebAssistantsFactory] = None):
        super().__init__(trading_pairs)

        self._api_factory: WebAssistantsFactory = api_factory or build_api_factory()
        self._last_ws_message_sent_timestamp = 0
        self._websocket_connection: Optional[Connection] = None
        self._snapshot_msg: Dict[str, any] = {}

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:
            async with await client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}", timeout=API_CALL_TIMEOUT) as response:
                response_json = await response.json()
                results = response_json['result']
                return {convert_from_exchange_trading_pair(result['name']): float(result['last'])
                        for result in results if convert_from_exchange_trading_pair(result['name']) in trading_pairs}

    async def get_trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        resp = requests.get(url=f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}/{convert_to_exchange_trading_pair(trading_pair)}")
        record = resp.json()["result"]
        result = (Decimal(record.get("bid", "0")) + Decimal(record.get("ask", "0"))) / Decimal("2")
        return result if result else None

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            api_factory = build_api_factory()
            rest_assistant = await api_factory.get_rest_assistant()

            url = f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}"
            request = RESTRequest(
                method=RESTMethod.GET,
                url=url
            )

            response = await rest_assistant.call(request=request)
            if response.status == 200:
                all_trading_pairs: Dict[str, Any] = await response.json()
                valid_trading_pairs: list = []
                for item in all_trading_pairs["result"]:
                    if item["type"] == "future" and item["futureType"] == "perpetual":
                        valid_trading_pairs.append(item["name"])
                trading_pair_list: List[str] = []
                for raw_trading_pair in valid_trading_pairs:
                    converted_trading_pair: Optional[str] = convert_from_exchange_trading_pair(raw_trading_pair)
                    if converted_trading_pair is not None:
                        trading_pair_list.append(converted_trading_pair)
                return trading_pair_list
        except Exception:
            pass

        return []

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    # async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
    #     async with client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}/{convert_to_exchange_trading_pair(trading_pair)}/orderbook?depth=100") as response:
    #         response: aiohttp.ClientResponse = response
    #         if response.status != 200:
    #             raise IOError(f"Error fetching FTX market snapshot for {trading_pair}. "
    #                           f"HTTP status is {response.status}.")
    #         data: Dict[str, Any] = simplejson.loads(await response.text(), parse_float=Decimal)

    #         return data

    # async def get_new_order_book(self, trading_pair: str) -> OrderBook:
    #     async with aiohttp.ClientSession() as client:
    #         snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
    #         snapshot_timestamp: float = time.time()
    #         snapshot_msg: OrderBookMessage = FtxPerpOrderBook.restful_snapshot_message_from_exchange(
    #             snapshot,
    #             snapshot_timestamp,
    #             metadata={"trading_pair": trading_pair}
    #         )
    #         order_book: OrderBook = self.order_book_create_function()
    #         order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
    #         return order_book

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_data: Dict[str, Any] = snapshot_response["result"]
        snapshot_timestamp: float = self._time()
        update_id: int = int(snapshot_timestamp * 1e3)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(price, amount) for price, amount in snapshot_data.get("bids", [])],
            "asks": [(price, amount) for price, amount in snapshot_data.get("asks", [])],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        symbol = convert_to_exchange_trading_pair(trading_pair)
        params = {"depth": "100"}

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.FTX_ORDER_BOOK_PATH.format(symbol)),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.FTX_ORDER_BOOK_LIMIT_ID,
        )

        return data

    # async def _inner_messages(self,
    #                           ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
    #     # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
    #     try:
    #         while True:
    #             try:
    #                 msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
    #                 yield msg
    #             except asyncio.TimeoutError:
    #                 pong_waiter = await ws.ping()
    #                 await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
    #     except asyncio.TimeoutError:
    #         self.logger().warning("WebSocket ping timed out. Going to reconnect...")
    #         return
    #     except ConnectionClosed:
    #         return
    #     finally:
    #         await ws.close()

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_updates = raw_message["data"]
        trading_pair = convert_from_exchange_trading_pair(raw_message["market"])

        for trade_data in trade_updates:
            message_content = {
                "trade_id": trade_data["id"],
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(
                    TradeType.SELL.value),
                "amount": trade_data["size"],
                "price": trade_data["price"]
            }
            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=datetime.fromisoformat(trade_data["time"]).timestamp())

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_message(
            self,
            raw_message: Dict[str, Any],
            message_queue: asyncio.Queue,
            message_type: OrderBookMessageType):
        diff_data: Dict[str, Any] = raw_message["data"]

        timestamp: float = diff_data["time"]
        update_id: int = int(timestamp * 1e3)
        trading_pair = convert_from_exchange_trading_pair(raw_message["market"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(price, amount) for price, amount in diff_data.get("bids", [])],
            "asks": [(price, amount) for price, amount in diff_data.get("asks", [])],
        }
        diff_message: OrderBookMessage = OrderBookMessage(
            message_type,
            order_book_message_content,
            timestamp)

        message_queue.put_nowait(diff_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        await self._parse_order_book_message(
            raw_message=raw_message,
            message_queue=message_queue,
            message_type=OrderBookMessageType.DIFF)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        await self._parse_order_book_message(
            raw_message=raw_message,
            message_queue=message_queue,
            message_type=OrderBookMessageType.SNAPSHOT)

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                payload = {
                    "op": "subscribe",
                    "channel": CONSTANTS.WS_TRADES_CHANNEL,
                    "market": convert_to_exchange_trading_pair(trading_pair)
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload)

                payload = {
                    "op": "subscribe",
                    "channel": CONSTANTS.WS_ORDER_BOOK_CHANNEL,
                    "market": convert_to_exchange_trading_pair(trading_pair)
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)

                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_REQUEST_LIMIT_ID):
                    await ws.send(subscribe_trade_request)
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_REQUEST_LIMIT_ID):
                    await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public orderbook and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...", exc_info=True
            )
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event_type = event_message["type"]
        channel = None
        if event_type == "error":
            raise IOError(f"An error occurred processing an event message in the order book data source "
                          f"(code: {event_message.get('code')}, message: {event_message.get('msg')})")
        elif event_type in ["partial", "update"]:
            event_channel = event_message["channel"]
            if event_channel == CONSTANTS.WS_TRADES_CHANNEL:
                channel = self._trade_messages_queue_key
            elif event_channel == CONSTANTS.WS_ORDER_BOOK_CHANNEL and event_type == "update":
                channel = self._diff_messages_queue_key
            elif event_channel == CONSTANTS.WS_ORDER_BOOK_CHANNEL and event_type == "partial":
                channel = self._snapshot_messages_queue_key

        return channel

    # async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
    #     while True:
    #         try:
    #             async with websockets.connect(FTX_WS_FEED) as ws:
    #                 ws: websockets.WebSocketClientProtocol = ws
    #                 for pair in self._trading_pairs:
    #                     subscribe_request: Dict[str, Any] = {
    #                         "op": "subscribe",
    #                         "channel": "trades",
    #                         "market": convert_to_exchange_trading_pair(pair)
    #                     }
    #                     await ws.send(ujson.dumps(subscribe_request))
    #                 async for raw_msg in self._inner_messages(ws):
    #                     msg = simplejson.loads(raw_msg, parse_float=Decimal)
    #                     if "channel" in msg:
    #                         if msg["channel"] == "trades" and msg["type"] == "update":
    #                             for trade in msg["data"]:
    #                                 trade_msg: OrderBookMessage = FtxPerpOrderBook.trade_message_from_exchange(msg, trade)
    #                                 output.put_nowait(trade_msg)
    #         except asyncio.CancelledError:
    #             raise
    #         except Exception:
    #             self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
    #                                 exc_info=True)
    #             await asyncio.sleep(30.0)

    # async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
    #     while True:
    #         try:
    #             async with websockets.connect(FTX_WS_FEED) as ws:
    #                 ws: websockets.WebSocketClientProtocol = ws
    #                 for pair in self._trading_pairs:
    #                     subscribe_request: Dict[str, Any] = {
    #                         "op": "subscribe",
    #                         "channel": "orderbook",
    #                         "market": convert_to_exchange_trading_pair(pair)
    #                     }
    #                     await ws.send(ujson.dumps(subscribe_request))
    #                 async for raw_msg in self._inner_messages(ws):
    #                     msg = simplejson.loads(raw_msg, parse_float=Decimal)
    #                     if "channel" in msg:
    #                         if msg["channel"] == "orderbook" and msg["type"] == "update":
    #                             order_book_message: OrderBookMessage = FtxPerpOrderBook.diff_message_from_exchange(msg, msg["data"]["time"])
    #                             output.put_nowait(order_book_message)
    #         except asyncio.CancelledError:
    #             raise
    #         except Exception:
    #             self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
    #                                 exc_info=True)
    #             await asyncio.sleep(30.0)

    # async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
    #     while True:
    #         try:
    #             async with websockets.connect(FTX_WS_FEED) as ws:
    #                 ws: websockets.WebSocketClientProtocol = ws
    #                 for pair in self._trading_pairs:
    #                     subscribe_request: Dict[str, Any] = {
    #                         "op": "subscribe",
    #                         "channel": "orderbook",
    #                         "market": convert_to_exchange_trading_pair(pair)
    #                     }
    #                     await ws.send(ujson.dumps(subscribe_request))
    #                 async for raw_msg in self._inner_messages(ws):
    #                     msg = simplejson.loads(raw_msg, parse_float=Decimal)
    #                     if "channel" in msg:
    #                         if msg["channel"] == "orderbook" and msg["type"] == "partial":
    #                             order_book_message: OrderBookMessage = FtxPerpOrderBook.snapshot_message_from_exchange(msg, msg["data"]["time"])
    #                             output.put_nowait(order_book_message)
    #         except asyncio.CancelledError:
    #             raise
    #         except Exception:
    #             self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
    #                                 exc_info=True)
    #             await asyncio.sleep(30.0)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                seconds_until_next_ping = (CONSTANTS.WS_PING_INTERVAL
                                           - (self._time() - self._last_ws_message_sent_timestamp))
                await asyncio.wait_for(super()._process_websocket_messages(websocket_assistant=websocket_assistant),
                                       timeout=seconds_until_next_ping)
            except asyncio.TimeoutError:
                payload = {"op": "ping"}
                ping_request = WSJSONRequest(payload=payload)
                self._last_ws_message_sent_timestamp = self._time()
                await websocket_assistant.send(request=ping_request)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await ws.connect(
                ws_url=CONSTANTS.FTX_WS_URL,
                message_timeout=CONSTANTS.WS_PING_INTERVAL)
        return ws

