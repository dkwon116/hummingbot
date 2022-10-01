import asyncio
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.korbit import korbit_constants as CONSTANTS, korbit_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.korbit.korbit_exchange import KorbitExchange


class KorbitAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'KorbitExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._last_ws_message_sent_timestamp = 0

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_data: Dict[str, Any] = snapshot_response
        snapshot_timestamp: float = self._time()
        update_id: int = int(snapshot_timestamp * 1e3)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(price, amount) for price, amount, _ in snapshot_data.get("bids", [])],
            "asks": [(price, amount) for price, amount, _ in snapshot_data.get("asks", [])],
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
        params = {"currency_pair": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)}

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.KORBIT_ORDER_BOOK_PATH),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.KORBIT_ORDER_BOOK_PATH,
        )

        return data

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_data = raw_message["data"]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["currency_pair"])

        message_content = {
            "trade_id": trade_data["timestamp"],
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.BUY.value) if trade_data["taker"] == "buy" else float(
                TradeType.SELL.value),
            "amount": trade_data["amount"],
            "price": trade_data["price"]
        }
        trade_message: Optional[OrderBookMessage] = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=message_content,
            timestamp=trade_data["timestamp"])

        message_queue.put_nowait(trade_message)

    async def _parse_order_book_message(
            self,
            raw_message: Dict[str, Any],
            message_queue: asyncio.Queue,
            message_type: OrderBookMessageType):
        diff_data: Dict[str, Any] = raw_message["data"]

        timestamp: float = diff_data["timestamp"]
        update_id: int = int(timestamp * 1e3)
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=diff_data["currency_pair"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [(bid["price"], bid["amount"]) for bid in diff_data.get("bids", [])],
            "asks": [(ask["price"], ask["amount"]) for ask in diff_data.get("asks", [])],
        }
        diff_message: OrderBookMessage = OrderBookMessage(
            message_type,
            order_book_message_content,
            timestamp)

        message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        await self._parse_order_book_message(
            raw_message=raw_message,
            message_queue=message_queue,
            message_type=OrderBookMessageType.SNAPSHOT)

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

                payload = {
                    "accessToken": None,
                    "timestamp": int(time.time() * 1000),
                    "event": "korbit:subscribe",
                    "data": {
                        "channels": [f"{CONSTANTS.WS_TRADES_CHANNEL}:{symbol}"]
                    }
                }

                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload)

                payload = {
                    "accessToken": None,
                    "timestamp": int(time.time() * 1000),
                    "event": "korbit:subscribe",
                    "data": {
                        "channels": [f"{CONSTANTS.WS_ORDER_BOOK_CHANNEL}:{symbol}"]
                    }
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)

            self._last_ws_message_sent_timestamp = self._time()
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book trading and delta streams...")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        _, event_type = event_message["event"].split(":")

        channel = None
        if event_type == "error":
            error_msg = event_message["data"]
            raise IOError(f"An error occurred processing an event message in the order book data source "
                          f"(error for {error_msg.get('request_event')}, message: {error_msg.get('err_message')})")
        elif event_type in [f"push-{CONSTANTS.WS_TRADES_CHANNEL}", f"push-{CONSTANTS.WS_ORDER_BOOK_CHANNEL}"]:
            event_channel = event_message["data"]["channel"]
            channel = self._trade_messages_queue_key if event_channel == CONSTANTS.WS_TRADES_CHANNEL else self._snapshot_messages_queue_key

        return channel

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                seconds_until_next_ping = (CONSTANTS.WS_PING_INTERVAL
                                           - (self._time() - self._last_ws_message_sent_timestamp))
                await asyncio.wait_for(super()._process_websocket_messages(websocket_assistant=websocket_assistant),
                                       timeout=seconds_until_next_ping)
            except asyncio.TimeoutError:
                self._last_ws_message_sent_timestamp = self._time()
                await websocket_assistant.ping()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.KORBIT_WS_URL,
            message_timeout=CONSTANTS.WS_PING_INTERVAL)
        return ws
