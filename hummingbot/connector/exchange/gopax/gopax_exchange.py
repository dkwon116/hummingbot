import logging
# import copy
from typing import TYPE_CHECKING, Any, AsyncIterable, Dict, List, Optional
from decimal import Decimal
import asyncio
import json
import aiohttp
import math
import time
from async_timeout import timeout

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.logger import HummingbotLogger
from hummingbot.core.clock import Clock
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OpenOrder, OrderType, TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketOrderFailureEvent
)
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange.gopax.gopax_order_book_tracker import GopaxOrderBookTracker
from hummingbot.connector.exchange.gopax.gopax_api_order_book_data_source import GopaxAPIOrderBookDataSource
from hummingbot.connector.exchange.gopax.gopax_user_stream_tracker import GopaxUserStreamTracker
from hummingbot.connector.exchange.gopax.gopax_websocket_adaptor import GopaxWebSocketAdaptor
from hummingbot.connector.exchange.gopax.gopax_auth import GopaxAuth
from hummingbot.connector.exchange.gopax.gopax_in_flight_order import GopaxInFlightOrder
from hummingbot.connector.exchange.gopax import gopax_constants as CONSTANTS, gopax_utils
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
# from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

ctce_logger = None
s_decimal_NaN = Decimal("nan")
s_decimal_0 = Decimal("0")


class GopaxExchange(ExchangeBase):
    """
    GopaxExchange connects with Crypto.com exchange and provides order book pricing, user account tracking and
    trading functionality.
    """
    API_CALL_TIMEOUT = 10.0
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    UPDATE_TRADING_RULES_INTERVAL = 60.0
    LONG_POLL_INTERVAL = 120.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global ctce_logger
        if ctce_logger is None:
            ctce_logger = logging.getLogger(__name__)
        return ctce_logger

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 gopax_api_key: str,
                 gopax_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True
                 ):
        """
        :param gopax_api_key: The API key to connect to private Crypto.com APIs.
        :param gopax_secret_key: The API secret.
        :param trading_pairs: The market trading pairs which to track order book data.
        :param trading_required: Whether actual trading is needed.
        """
        super().__init__(client_config_map)
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._gopax_auth = GopaxAuth(gopax_api_key, gopax_secret_key)
        self._shared_client = aiohttp.ClientSession()
        self._throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)

        self._order_book_tracker = GopaxOrderBookTracker(
            auth_assistant=self._gopax_auth,
            shared_client=self._shared_client,
            throttler=self._throttler,
            trading_pairs=trading_pairs
        )
        self._user_stream_tracker = GopaxUserStreamTracker(
            throttler=self._throttler, shared_client=self._shared_client, auth_assistant=self._gopax_auth
        )
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._in_flight_orders = {}  # Dict[client_order_id:str, GopaxInFlightOrder]
        self._order_not_found_records = {}  # Dict[client_order_id:str, count:int]
        self._trading_rules = {}  # Dict[trading_pair:str, TradingRule]
        self._last_poll_timestamp = 0

        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, GopaxInFlightOrder]:
        return self._in_flight_orders

    @property
    def status_dict(self) -> Dict[str, bool]:
        """
        A dictionary of statuses of various connector's components.
        """
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "user_stream_initialized": self._user_stream_tracker.data_source.last_recv_time > 0 if self._trading_required else True,
        }

    @property
    def ready(self) -> bool:
        """
        :return True when all statuses pass, this might take 5-10 seconds for all the connector's components and
        services to be ready.
        """
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        """
        :return active in-flight orders in json format, is used to save in sqlite db.
        """
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
            if not value.is_done
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        """
        Restore in-flight orders from saved tracking states, this is st the connector can pick up on where it left off
        when it disconnects.
        :param saved_states: The saved tracking_states.
        """
        self._in_flight_orders.update({
            key: GopaxInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector.
        Note that Market order type is no longer required and will not be used.
        """
        return [OrderType.LIMIT]

    def start(self, clock: Clock, timestamp: float):
        """
        This function is called automatically by the clock.
        """
        print("Start by the clock")
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        """
        This function is called automatically by the clock.
        """
        super().stop(clock)

    async def start_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        It starts tracking order book, polling trading rules,
        updating statuses and tracking user data.
        """
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def stop_network(self):
        """
        This function is required by NetworkIterator base class and is called automatically.
        """
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        """
        This function is required by NetworkIterator base class and is called periodically to check
        the network connection. Simply ping the network (or call any light weight public API).
        """
        try:
            # since there is no ping endpoint, the lowest rate call is to get BTC-USDT ticker
            await self._api_request("get", CONSTANTS.GET_TIME_URL)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def _trading_rules_polling_loop(self):
        """
        Periodically update trading rule.
        """
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(self.UPDATE_TRADING_RULES_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(f"Unexpected error while fetching trading rules. Error: {str(e)}",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Gopax. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _update_trading_rules(self):
        """
        Gopax does not give all market trading rule in single call.
        Trading rule not really provided by API
        Might be better to manually create trading rule rather than from API
        """
        
        self.logger().info(f"Update trading rules for {self._trading_pairs}")

        instrument_info: List[Dict[str, Any]] = await self._api_request("get", CONSTANTS.GET_TICKER_URL, {}, True)
        self._trading_rules.clear()
        self._trading_rules = self._format_trading_rules(instrument_info)

    def _format_trading_rules(self, markets: Dict[str, Any]) -> Dict[str, TradingRule]:
        """
        From list of markets received create dictionary of trading rules.
        Minimum price increments and base amount increments mainly needed
        :param markets: The json API response of markets
        :return A dictionary of trading rules.
        Response Example:
        {
            "id": 11,
            "method": "public/get-instruments",
            "code": 0,
            "result": {
                "instruments": [
                      {
                        "instrument_name": "ETH_CRO",
                        "quote_currency": "CRO",
                        "base_currency": "ETH",
                        "price_decimals": 2,
                        "quantity_decimals": 2
                      },
                      {
                        "instrument_name": "CRO_BTC",
                        "quote_currency": "BTC",
                        "base_currency": "CRO",
                        "price_decimals": 8,
                        "quantity_decimals": 2
                      }
                    ]
              }
        }
        """
        result = {}
        trading_pairs = [pair for pair in self._trading_pairs]
        markets = [market for market in markets if market["name"] in trading_pairs]
        
        for pair in markets:
            try:
                # From orderbook get current price
                pair_name = pair["name"]
                current_price = pair["close"]
                self.logger().debug(f"Formatting trade rule for {pair_name} with current price of {current_price}")

                price_step = Decimal(str(gopax_utils.get_price_increments(current_price)))
                quantity_step = Decimal("1") / Decimal(str(math.pow(10, 8)))
                min_order_value = Decimal("11000")
                min_order_size = min_order_value / Decimal(str(current_price))

                result[pair_name] = TradingRule(
                    pair_name,
                    min_price_increment=price_step,
                    min_base_amount_increment=quantity_step,
                    min_quote_amount_increment=price_step,
                    min_order_size=min_order_size,
                    min_order_value=min_order_value,
                    min_notional_size=min_order_value
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {pair_name}. Skipping.", exc_info=True)
        return result

    async def _api_request(self,
                           method: str,
                           path_url: str,
                           params: Dict[str, Any] = {},
                           is_auth_required: bool = False) -> Dict[str, Any]:
        """
        Sends an aiohttp request and waits for a response.
        :param method: The HTTP method, e.g. get or post
        :param path_url: The path url or the API end point
        :param is_auth_required: Whether an authentication is required, when True the function will add encrypted
        signature to the request.
        :returns A response in json format.
        """
        async with self._throttler.execute_task(path_url):
            if "path_append" in params:
                path_url += params["path_append"]
                del params["path_append"]
            url = gopax_utils.get_rest_url(path_url)
            if method == "get" and path_url in [CONSTANTS.GET_OPEN_ORDERS_PATH_URL, CONSTANTS.GET_TRADES_HISTORY_PATH_URL]:
                url = gopax_utils.get_url_with_param(url, params)
            client = await self._http_client()
            headers = self._gopax_auth.get_auth_headers(is_auth_required, method, path_url, params, 5000)

            if method == "get":
                response = await client.get(url, headers=headers)
            elif method == "post":
                post_json = json.dumps(params)
                response = await client.post(url, data=post_json, headers=headers)
            elif method == "delete":
                post_json = json.dumps(params)
                response = await client.delete(url, headers=headers)
            else:
                raise NotImplementedError

            try:
                parsed_response = json.loads(await response.text())
                
            except Exception as e:
                raise self.logger().warning(f"Error parsing data from {url}. Error: {str(e)}")
            if response.status not in [200, 201]:
                if parsed_response["errorCode"] == 10069:
                    self._process_order_not_found(parsed_response["errorData"])
                raise self.logger().warning(f"Error fetching data from {url}. HTTP status is {response.status}. Message: {parsed_response}")
            if "error" in parsed_response:
                raise self.logger().warning(f"{url} API call failed, response: {parsed_response['message']}")

            return parsed_response

    def get_order_price_quantum(self, trading_pair: str, price: Decimal):
        """
        Returns a price step, a minimum price increment for a given trading pair.
        """
        # trading_rule = self._trading_rules[trading_pair]
        return Decimal(str(gopax_utils.get_price_increments(price)))

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal):
        """
        Returns an order amount step, a minimum amount increment for a given trading pair.
        """
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def get_min_order_size(self, trading_pair: str):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_order_size)

    def quantize_order_amount(self, trading_pair, amount, price = Decimal(str(0.0))):
        trading_rule = self._trading_rules[trading_pair]
        quantized_amount = ExchangeBase.quantize_order_amount(self, trading_pair, amount)

        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        if price == s_decimal_0:
            current_price = self.get_price(trading_pair, False)
            notional_size = current_price * quantized_amount
        else:
            notional_size = price * quantized_amount

        # Add 1% as a safety factor in case the prices changed while making the order.
        if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
            return s_decimal_0

        return quantized_amount

    def get_order_book(self, trading_pair: str) -> OrderBook:
        if trading_pair not in self._order_book_tracker.order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return self._order_book_tracker.order_books[trading_pair]

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Buys an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for BuyOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to buy from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = gopax_utils.get_new_client_order_id(True, trading_pair)
        safe_ensure_future(self._create_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price))
        return order_id

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        """
        Sells an amount of base asset (of the given trading pair). This function returns immediately.
        To see an actual order, you'll have to wait for SellOrderCreatedEvent.
        :param trading_pair: The market (e.g. BTC-USDT) to sell from
        :param amount: The amount in base token value
        :param order_type: The order type
        :param price: The price (note: this is no longer optional)
        :returns A new internal order id
        """
        order_id: str = gopax_utils.get_new_client_order_id(False, trading_pair)
        safe_ensure_future(self._create_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price))
        return order_id

    def cancel(self, trading_pair: str, order_id: str):
        """
        Cancel an order. This function returns immediately.
        To get the cancellation result, you'll have to wait for OrderCancelledEvent.
        :param trading_pair: The market (e.g. BTC-USDT) of the order.
        :param order_id: The internal order id (also called client_order_id)
        """
        safe_ensure_future(self._execute_cancel(trading_pair, order_id))
        return order_id

    async def _create_order(self,
                            trade_type: TradeType,
                            order_id: str,
                            trading_pair: str,
                            amount: Decimal,
                            order_type: OrderType,
                            price: Decimal):
        """
        Calls create-order API end point to place an order, starts tracking the order and triggers order created event.
        :param trade_type: BUY or SELL
        :param order_id: Internal order id (also called client_order_id)
        :param trading_pair: The market to place order
        :param amount: The order amount (in base token value)
        :param order_type: The order type
        :param price: The order price
        """
        if not order_type.is_limit_type():
            raise Exception(f"Unsupported order type: {order_type}")
        trading_rule = self._trading_rules[trading_pair]

        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)
        order_value = amount * price
        if order_value < trading_rule.min_order_value:
            raise ValueError(f"Buy order amount {order_value} is lower than the minimum order value "
                             f"{trading_rule.min_order_value}.")

        api_params = {
            "clientOrderId": order_id,
            "tradingPairName": trading_pair,
            "side": "buy" if trade_type.name == "BUY" else "sell",
            "type": "limit",
            "price": f"{price:f}",
            "amount": f"{amount:f}"
        }

        self.start_tracking_order(order_id, None, trading_pair, trade_type, price, amount, order_type)

        try:
            order_result = await self._api_request("post", CONSTANTS.CREATE_ORDER_PATH_URL, api_params, True)
            exchange_order_id = str(order_result["id"])
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type.name} {trade_type.name} order {order_id} for "
                                   f"{amount} {trading_pair}.")
                tracked_order.update_exchange_order_id(exchange_order_id)

            event_tag = MarketEvent.BuyOrderCreated if trade_type is TradeType.BUY else MarketEvent.SellOrderCreated
            event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
            self.trigger_event(event_tag,
                               event_class(
                                   self.current_timestamp,
                                   order_type,
                                   trading_pair,
                                   amount,
                                   price,
                                   order_id,
                                   tracked_order.creation_timestamp,
                                   exchange_order_id
                               ))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting {trade_type.name} {order_type.name} order to Gopax for "
                f"{amount} {trading_pair} "
                f"{price}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal,
                             order_type: OrderType):
        """
        Starts tracking an order by simply adding it into _in_flight_orders dictionary.
        """
        self._in_flight_orders[order_id] = GopaxInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            creation_timestamp=self.current_timestamp
        )

    def stop_tracking_order(self, order_id: str):
        """
        Stops tracking an order by simply removing it from _in_flight_orders dictionary.
        """
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    async def _execute_cancel(self, trading_pair: str, order_id: str) -> str:
        """
        Executes order cancellation process by first calling cancel-order API. The API result doesn't confirm whether
        the cancellation is successful, it simply states it receives the request.
        :param trading_pair: The market trading pair
        :param order_id: The internal order id
        order.last_state to change to CANCELED
        """
        try:
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is None:
                self._process_order_not_found(client_order_id = order_id)
                raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
            if tracked_order.exchange_order_id is None:
                await tracked_order.get_exchange_order_id()
            ex_order_id = tracked_order.exchange_order_id
            await self._api_request("delete", CONSTANTS.CANCEL_ORDER_PATH_URL, {"path_append": f"/{ex_order_id}"}, True)
            return order_id
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Gopax. "
                                f"Check API key and network connection."
            )

    async def _status_polling_loop(self):
        """
        Periodically update user balances and order status via REST API.
        Gopax does not have User Stream rely only on REST API
        """
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(str(e), exc_info=True)
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Crypto.com. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _update_balances(self):
        """
        Calls REST API to update total and available balances.
        """
        self.logger().debug(f"Update account balance")
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()
        account_info = await self._api_request("get", CONSTANTS.GET_ACCOUNT_SUMMARY_PATH_URL, {}, True)
        for asset in account_info:
            asset_name = asset["asset"]
            self._account_available_balances[asset_name] = Decimal(str(asset["avail"]))
            self._account_balances[asset_name] = Decimal(str(asset["avail"])) + Decimal(str(asset["hold"]))
            remote_asset_names.add(asset_name)
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _update_order_status(self):
        """
        Calls REST API to get status update for each in-flight order.
        """
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            tasks = []
            for tracked_order in tracked_orders:
                order_id = await tracked_order.get_exchange_order_id()
                tasks.append(self._api_request("get", CONSTANTS.GET_ORDER_DETAIL_PATH_URL, {"path_append": f"/{order_id}"}, True))
            self.logger().info(f"Polling for order status updates of {len(tasks)} orders.")
            order_response = await safe_gather(*tasks, return_exceptions=True)

            parsed_order_response: List[Dict[str, Any]] = []
            for resp in order_response:
                if not isinstance(resp, Exception):
                    parsed_order_response.append(resp)
                else:
                    self.logger().error(f"Error fetching order status. Response: {resp}")

            if len(parsed_order_response) == 0:
                return

            trade_history_tasks = []
            for trading_pair in self._trading_pairs:
                query_string = f"?tradingPairName={trading_pair}&after={self._last_poll_timestamp - self.UPDATE_ORDER_STATUS_MIN_INTERVAL*2}"
                trade_history_tasks.append(
                    asyncio.create_task(self._api_request(
                        method="get",
                        path_url=CONSTANTS.GET_TRADES_HISTORY_PATH_URL,
                        params={"path_append": query_string},
                        is_auth_required=True)))

            trade_responses: List[Dict[str, Any]] = await safe_gather(*trade_history_tasks, return_exceptions=True)

            parsed_history_resps: List[Dict[str, Any]] = []
            for resp in trade_responses:
                if not isinstance(resp, Exception):
                    parsed_history_resps.extend(resp)
                else:
                    self.logger().error(f"Error fetching trades history. Response: {resp}")

            # Trade updates must be handled before any order status updates.
            for trade in parsed_history_resps:
                await self._process_trade_message(trade)

            for order_status in parsed_order_response:
                self._process_order_message(order_status)

    def _process_order_not_found(self, exchange_order_id = None, client_order_id = None):
        exchange_order_id = str(exchange_order_id)

        for in_flight_order in self._in_flight_orders.values():
            if in_flight_order.exchange_order_id == exchange_order_id:
                client_order_id = in_flight_order.client_order_id

        if client_order_id is None:
            return
        
        tracked_order = self._in_flight_orders[client_order_id]
        self.logger().info(f"Order Not Found. Successfully cancelled order {client_order_id}.")
        self.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(self.current_timestamp, client_order_id))
        tracked_order.cancelled_event.set()
        self.stop_tracking_order(client_order_id)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancellation or failure event if needed.
        :param order_msg: The order response from either REST or web socket API (they are of the same format)
        """
        exchange_order_id = str(order_msg["id"])
        client_order_id = None

        for in_flight_order in self._in_flight_orders.values():
            if in_flight_order.exchange_order_id == exchange_order_id:
                client_order_id = in_flight_order.client_order_id

        if client_order_id is None:
            return

        tracked_order = self._in_flight_orders[client_order_id]
        # Update order execution status
        tracked_order.last_state = order_msg["status"]
        if tracked_order.is_cancelled:
            self.logger().info(f"Successfully cancelled order {client_order_id}.")
            self.trigger_event(MarketEvent.OrderCancelled,
                               OrderCancelledEvent(
                                   self.current_timestamp,
                                   client_order_id))
            tracked_order.cancelled_event.set()
            self.stop_tracking_order(client_order_id)
        elif tracked_order.is_failure:
            self.logger().info(f"The market order {client_order_id} has failed according to order status API. ")
            self.trigger_event(MarketEvent.OrderFailure,
                               MarketOrderFailureEvent(
                                   self.current_timestamp,
                                   client_order_id,
                                   tracked_order.order_type
                               ))
            self.stop_tracking_order(client_order_id)

    async def _process_trade_message(self, trade_msg: Dict[str, Any]):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        """
        for order in self._in_flight_orders.values():
            await order.get_exchange_order_id()
        track_order = [o for o in self._in_flight_orders.values() if str(trade_msg["orderId"]) == o.exchange_order_id]
        if not track_order:
            return
        tracked_order = track_order[0]
        fee_paid = Decimal(str(trade_msg["fee"]))
        updated = tracked_order.update_with_trade_update(trade_msg)

        if not updated:
            return
        self.trigger_event(
            MarketEvent.OrderFilled,
            OrderFilledEvent(
                self.current_timestamp,
                tracked_order.client_order_id,
                tracked_order.trading_pair,
                tracked_order.trade_type,
                tracked_order.order_type,
                Decimal(str(trade_msg["price"])),
                Decimal(str(trade_msg["baseAmount"])),
                # TradeFee(0.0, [(tracked_order.quote_asset, Decimal(str(fee_paid)))]),
                AddedToCostTradeFee(
                    flat_fees=[TokenAmount(tracked_order.quote_asset, fee_paid)]
                ),
                exchange_trade_id=trade_msg["id"]
            )
        )
        if math.isclose(tracked_order.executed_amount_base, tracked_order.amount) or \
                tracked_order.executed_amount_base >= tracked_order.amount:
            tracked_order.last_state = "done"
            self.logger().info(f"The {tracked_order.trade_type.name} order "
                               f"{tracked_order.client_order_id} has completed "
                               f"according to order status API.")
            event_tag = MarketEvent.BuyOrderCompleted if tracked_order.trade_type is TradeType.BUY \
                else MarketEvent.SellOrderCompleted
            event_class = BuyOrderCompletedEvent if tracked_order.trade_type is TradeType.BUY \
                else SellOrderCompletedEvent
            self.trigger_event(event_tag,
                               event_class(self.current_timestamp,
                                           tracked_order.client_order_id,
                                           tracked_order.base_asset,
                                           tracked_order.quote_asset,
                                           tracked_order.executed_amount_base,
                                           tracked_order.executed_amount_quote,
                                           tracked_order.order_type))
            self.stop_tracking_order(tracked_order.client_order_id)

    async def cancel_all(self, timeout_seconds: float):
        """
        Cancels all in-flight orders and waits for cancellation results.
        Used by bot's top level stop and exit commands (cancelling outstanding orders on exit)
        :param timeout_seconds: The timeout at which the operation will be canceled.
        :returns List of CancellationResult which indicates whether each order is successfully cancelled.
        """
        incomplete_orders = [order for order in self._in_flight_orders.values() if not order.is_done]
        tracked_orders = self._in_flight_orders.copy()
        tasks = [self._execute_cancel(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        cancellation_results = []

        try:
            async with timeout(timeout_seconds):
                await safe_gather(*tasks, return_exceptions=True)

            open_orders = await self.get_open_orders()

            for cl_order_id, tracked_order in tracked_orders.items():
                open_order = [o for o in open_orders if o.client_order_id == cl_order_id]
                if not open_order:
                    cancellation_results.append(CancellationResult(cl_order_id, True))
                    self.trigger_event(MarketEvent.OrderCancelled,
                                       OrderCancelledEvent(self.current_timestamp, cl_order_id))
                    self.stop_tracking_order(cl_order_id)
                else:
                    cancellation_results.append(CancellationResult(cl_order_id, False))
        except Exception:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                app_warning_msg="Failed to cancel order on Gopax. Check API key and network connection."
            )

        return cancellation_results

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        """
        To get trading fee, this function is simplified by using fee override configuration. Most parameters to this
        function are ignore except order_type. Use OrderType.LIMIT_MAKER to specify you want trading fee for
        maker order.
        """
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def get_open_orders(self) -> List[OpenOrder]:
        result = await self._api_request("get", CONSTANTS.GET_OPEN_ORDERS_PATH_URL, {}, True)
        ret_val = []
        for order in result:
            # if gopax_utils.HBOT_BROKER_ID not in order["client_oid"]:
            #     continue
            if order["type"] != "LIMIT":
                raise Exception(f"Unsupported order type {order['type']}")
            ret_val.append(
                OpenOrder(
                    trading_pair=order["tradingPairName"],
                    price=Decimal(str(order["price"])),
                    amount=Decimal(str(order["amount"])),
                    executed_amount=Decimal(str(order["amount"] - order["remaining"])),
                    status=order["status"],
                    order_type=OrderType.LIMIT,
                    is_buy=True if order["side"].lower() == "buy" else False,
                    time=int(gopax_utils.dt_to_ts(order["createdAt"])),
                    exchange_order_id=order["id"]
                )
            )
        return ret_val

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from NDAX. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                endpoint = GopaxWebSocketAdaptor.endpoint_from_message(event_message)
                payload = GopaxWebSocketAdaptor.payload_from_message(event_message)

                if endpoint == GopaxWebSocketAdaptor.ACCOUNT_POSITION_EVENT_NAME:
                    self._process_account_position_event(payload)
                elif endpoint == GopaxWebSocketAdaptor.ORDER_STATE_EVENT_NAME:
                    payload = GopaxWebSocketAdaptor.convert_order_payload(payload)
                    self._process_order_message(payload)
                elif endpoint == GopaxWebSocketAdaptor.ORDER_TRADE_EVENT_NAME:
                    payload = GopaxWebSocketAdaptor.convert_trade_payload(payload)
                    await self._process_trade_message(payload)
                else:
                    self.logger().debug(f"Unknown event received from the connector ({event_message})")
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self.logger().error(f"Unexpected error in user stream listener loop ({ex})", exc_info=True)
                await asyncio.sleep(5.0)

    def _process_account_position_event(self, account_position_event: Dict[str, Any]):
        token = account_position_event["isoAlpha3"]
        amount = Decimal(str(account_position_event["avail"] + account_position_event["hold"]))
        self._account_balances[token] = amount
        self._account_available_balances[token] = Decimal(str(account_position_event["avail"]))

    async def all_trading_pairs(self) -> List[str]:
        # This method should be removed and instead we should implement _initialize_trading_pair_symbol_map
        return await GopaxAPIOrderBookDataSource.fetch_trading_pairs()
