import aiohttp
import asyncio
import logging
import time

from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import ujson

from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS, bithumb_utils
from hummingbot.connector.exchange.bithumb.bithumb_order_book import BithumbOrderBook
from hummingbot.connector.exchange.bithumb.bithumb_order_book_message import BithumbOrderBookMessage
from hummingbot.connector.exchange.bithumb.bithumb_utils import convert_to_exchange_trading_pair, convert_from_exchange_trading_pair, split_orderbook_to_bidask, convert_orderbook_ws, dt_to_ts
from hummingbot.connector.exchange.bithumb.bithumb_websocket_adaptor import BithumbWebSocketAdaptor
from hummingbot.core.data_type.order_book import OrderBook

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger.logger import HummingbotLogger


class BithumbAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _ORDER_BOOK_SNAPSHOT_DELAY = 60 * 60  # expressed in seconds

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_id_map: Dict[str, int] = {}
    _last_traded_prices: Dict[str, float] = {}

    def __init__(
        self,
        throttler: Optional[AsyncThrottler] = None,
        shared_client: Optional[aiohttp.ClientSession] = None,
        trading_pairs: Optional[List[str]] = None,
        domain: Optional[str] = None,
    ):
        super().__init__(trading_pairs)
        self._shared_client = shared_client or self._get_session_instance()
        self._throttler = throttler or self._get_throttler_instance()
        self._domain: Optional[str] = domain
        self._trading_pairs = trading_pairs

        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @classmethod
    def _get_session_instance(cls) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession()
        return session

    @classmethod
    def _get_throttler_instance(cls) -> AsyncThrottler:
        throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        return throttler

    @classmethod
    async def get_last_traded_prices(
        cls, trading_pairs: List[str], domain: Optional[str] = None, throttler: Optional[AsyncThrottler] = None, shared_client: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, float]:
        """Fetches the Last Traded Price of the specified trading pairs.

        :params: List[str] trading_pairs: List of trading pairs(in Hummingbot base-quote format i.e. BTC-CAD)
        :return: Dict[str, float]: Dictionary of the trading pairs mapped to its last traded price in float
        """

        shared_client = shared_client or cls._get_session_instance()

        results = {}
        pairs_to_update = []

        throttler = throttler or cls._get_throttler_instance()
        async with throttler.execute_task(CONSTANTS.GET_TICKER_URL):
            async with shared_client.get(
                f"{bithumb_utils.rest_api_url(domain) + CONSTANTS.GET_TICKER_URL}"
            ) as response:
                if response.status == 200:
                    resp_json = await response.json()

                    for t_pair in trading_pairs:
                        last_trade = [
                            resp_json["data"][pair]["closing_price"]
                            for pair in resp_json["data"]
                            if pair+"-KRW" == t_pair
                        ]
                    if last_trade and last_trade[0] is not None:
                        results[t_pair] = float(last_trade[0])

        return results

    @staticmethod
    async def fetch_trading_pairs(domain: str = None, throttler: Optional[AsyncThrottler] = None) -> List[str]:
        """Fetches and formats all supported trading pairs.

        Returns:
            List[str]: List of supported trading pairs in Hummingbot's format. (i.e. BASE-QUOTE)
        """
        async with aiohttp.ClientSession() as client:
            throttler = throttler or BithumbAPIOrderBookDataSource._get_throttler_instance()
            async with throttler.execute_task(CONSTANTS.GET_MARKETS_URL):
                async with client.get(
                    f"{bithumb_utils.rest_api_url(domain) + CONSTANTS.GET_MARKETS_URL}"
                ) as response:
                    if response.status == 200:
                        resp_json = await response.json()
                        return [pair+"-KRW" for pair in resp_json["data"] if pair != "date"]
                    return []

    async def get_snapshot(self, trading_pair: str, domain: Optional[str] = None, throttler: Optional[AsyncThrottler] = None) -> Dict[str, any]:
        """Retrieves entire orderbook snapshot of the specified trading pair via the REST API.

        Args:
            trading_pair (str): Trading pair of the particular orderbook.
            domain (str): The label of the variant of the connector that is being used.
            throttler (AsyncThrottler): API-requests throttler to use.

        Returns:
            Dict[str, any]: Parsed API Response.
        """

        pair = convert_to_exchange_trading_pair(trading_pair)
        throttler = throttler or self._get_throttler_instance()
        async with throttler.execute_task(CONSTANTS.GET_ORDER_BOOK_URL):
            url = f"{bithumb_utils.rest_api_url(domain) + CONSTANTS.GET_ORDER_BOOK_URL + pair}"
            async with self._shared_client.get(url) as response:
                status = response.status
                if status != 200:
                    raise IOError(
                        f"Error fetching OrderBook for {trading_pair} at {CONSTANTS.GET_ORDER_BOOK_URL}. "
                        f"HTTP {status}. Response: {await response.json()}"
                    )
                response_ls: Dict[str,Any] = await response.json()
                rsp = response_ls["data"]
                ts: float = int(rsp["timestamp"]) / 1000
                data = split_orderbook_to_bidask(rsp["bids"], rsp["asks"], ts)
                data["timestamp"] = ts
                return data

    async def _sleep(self, delay):
        """
        Function added only to facilitate patching the sleep in unit tests without affecting the asyncio module
        """
        await asyncio.sleep(delay)

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair, self._domain)

        snapshot_msg: BithumbOrderBookMessage = BithumbOrderBook.snapshot_message_from_exchange(
            msg=snapshot,
            trading_pair=trading_pair,
            timestamp=snapshot["timestamp"],
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def _create_websocket_connection(self) -> BithumbWebSocketAdaptor:
        """
        Initialize WebSocket client for APIOrderBookDataSource
        """
        try:
            ws = BithumbWebSocketAdaptor(shared_client=self._get_session_instance())
            await ws.connect()
            return ws
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            self.logger().network(f"Unexpected error occurred during {CONSTANTS.EXCHANGE_NAME} WebSocket Connection "
                                  f"({ex})")
            raise

    async def listen_for_subscriptions(self):
        ws = None
        while True:
            try:
                ws = await self._create_websocket_connection()
                await ws.subscribe_to_order_book_streams(self._trading_pairs)

                async for msg in ws.iter_messages():
                    if BithumbWebSocketAdaptor._msg_type_field_name not in msg:
                        continue
                    channel = msg[BithumbWebSocketAdaptor._msg_type_field_name]
                    if channel in BithumbWebSocketAdaptor.SUBSCRIPTION_LIST:
                        self._message_queue[channel].put_nowait(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.close()


    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket trade channel
        """
        msg_queue = self._message_queue[BithumbWebSocketAdaptor.TRADE_CHANNEL_ID]
        while True:
            try:
                payload = await msg_queue.get()
                msg = payload["content"]["list"][0]
                msg["contDtm"] = dt_to_ts(msg["contDtm"])
                trade_timestamp: int = msg["contDtm"]

                trade_msg: BithumbOrderBookMessage = BithumbOrderBook.trade_message_from_exchange(
                    msg,
                    trade_timestamp,
                    metadata={"trading_pair": convert_from_exchange_trading_pair(msg["symbol"])},
                )
                output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(f"Unexpected error parsing order book trade payload. Payload: {payload}",
                                    exc_info=True)
                await self._sleep(30.0)


    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook diffs using WebSocket API.
        """
        msg_queue = self._message_queue[BithumbWebSocketAdaptor.DIFF_CHANNEL_ID]
        while True:
            try:
                payload = await msg_queue.get()
                timestamp: float = int(payload["content"]["datetime"]) / 1000
                trading_pair = convert_from_exchange_trading_pair(payload["content"]["list"][0]["symbol"])
                order_book_data = convert_orderbook_ws(payload["content"]["list"], timestamp)
                
                orderbook_msg: BithumbOrderBookMessage = BithumbOrderBook.diff_message_from_exchange(
                    order_book_data,
                    timestamp,
                    metadata={"trading_pair": trading_pair},
                )
                output.put_nowait(orderbook_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    f"Unexpected error parsing order book diff payload. Payload: {payload}",
                    exc_info=True,
                )
                await self._sleep(30.0)
  

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Periodically polls for orderbook snapshots using the REST API.
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair, domain=self._domain)
                    snapshot_timestamp: int = snapshot["timestamp"]
                    metadata = {
                        "trading_pair": trading_pair,
                    }

                    snapshot_message: BithumbOrderBookMessage = BithumbOrderBook.snapshot_message_from_exchange(
                        msg=snapshot,
                        trading_pair=trading_pair,
                        timestamp=snapshot_timestamp,
                        metadata=metadata
                    )
                    output.put_nowait(snapshot_message)
                await self._sleep(self._ORDER_BOOK_SNAPSHOT_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error occured listening for orderbook snapshots. Retrying in 5 secs...",
                                    exc_info=True)
                await self._sleep(5.0)
