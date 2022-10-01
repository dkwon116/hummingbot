import asyncio
import math
import copy
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.exchange.korbit import korbit_constants as CONSTANTS, korbit_utils, korbit_web_utils as web_utils
from hummingbot.connector.exchange.korbit.korbit_api_order_book_data_source import KorbitAPIOrderBookDataSource
from hummingbot.connector.exchange.korbit.korbit_api_user_stream_data_source import KorbitAPIUserStreamDataSource
from hummingbot.connector.exchange.korbit.korbit_auth import KorbitAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class KorbitExchange(ExchangePyBase):
    web_utils = web_utils
    SHORT_POLL_INTERVAL = 1.0

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 korbit_api_key: str,
                 korbit_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        self._api_key = korbit_api_key
        self._secret_key = korbit_secret_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._auth_polling_task = None
        super().__init__(client_config_map)

        self._real_time_balance_update = True

    @property
    def name(self) -> str:
        return "korbit"

    @property
    def authenticator(self) -> AuthBase:
        return KorbitAuth(
            api_key=self._api_key,
            secret_key=self._secret_key)

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return CONSTANTS.DEFAULT_DOMAIN

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.KORBIT_MARKETS_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.KORBIT_MARKETS_PATH

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.KORBIT_NETWORK_STATUS_PATH

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "symbols_mapping_initialized": self.trading_pair_symbol_map_ready(),
            "order_books_initialized": self.order_book_tracker.ready,
            "account_balance": not self.is_trading_required or len(self._account_balances) > 0,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self.is_trading_required else True
        }

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        # KORBIT API does not include an endpoint to get the server time, thus the TimeSynchronizer is not used
        return False

    async def start_network(self):
        """
        Start all required tasks to update the status of the connector. Those tasks include:
        - The order book tracker
        - The polling loops to update the trading rules and trading fees
        - The polling loop to update order status and balance status using REST API (backup for main update process)
        - The background task to process the events received through the user stream tracker (websocket connection)
        """
        self._stop_network()
        self.order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        self._trading_fees_polling_task = safe_ensure_future(self._trading_fees_polling_loop())
        if self.is_trading_required:
            # self._auth_polling_task = safe_ensure_future(self.authenticator._auth_token_polling_loop())
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            # self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            # self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
            self._lost_orders_update_task = safe_ensure_future(self._lost_orders_update_polling_loop())

    async def stop_network(self):
        self._stop_network()
        if self._auth_polling_task is not None:
            self._auth_polling_task.cancel()
        self._auth_polling_task = None

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        exchange_order_id = await tracked_order.get_exchange_order_id()
        params = {
            "currency_pair": await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair),
            "id": exchange_order_id
        }
        cancel_result = await self._api_post(
            path_url=CONSTANTS.KORBIT_ORDERS_PATH + "/cancel",
            data=params,
            is_auth_required=True,
            limit_id=CONSTANTS.KORBIT_ORDERS_PATH)

        cancelled = False
        for result in cancel_result:
            cancelled = True if result["status"] == "success" and result["orderId"] == exchange_order_id else False

        if not cancelled:
            self.logger().info(
                f"Failed to cancel order {order_id} ({cancel_result})")

        return cancelled

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:

        api_params = {
            "currency_pair": await self.exchange_symbol_associated_to_pair(trading_pair),
            "type": "market" if order_type == OrderType.MARKET else "limit",
            "price": float(price),
            "coin_amount": float(amount)
        }

        order_result = await self._api_post(
            path_url=CONSTANTS.KORBIT_ORDERS_PATH + f"/{trade_type.name.lower()}",
            data=api_params,
            limit_id=CONSTANTS.KORBIT_ORDERS_PATH,
            is_auth_required=True)

        if order_result["status"] == "success":
            exchange_order_id = str(order_result["orderId"])
            return exchange_order_id, self.current_timestamp
        else:
            self.logger().info(f"Failed to create order-{order_result['status']})")
            raise Exception(f"order failure {order_result}")

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:

        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        pass

    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream_tracker.user_stream queue.
        Traders, Orders, and Balance updates from the WS.
        """
        pass
        # async for event_message in self._iter_user_event_queue():
        #     try:
        #         channel: str = event_message["channel"]
        #         data: Dict[str, Any] = event_message["data"]
        #         if channel == CONSTANTS.WS_PRIVATE_FILLS_CHANNEL:
        #             exchange_order_id = str(data["orderId"])
        #             order = next((order for order in self._order_tracker.all_fillable_orders.values()
        #                           if order.exchange_order_id == exchange_order_id),
        #                          None)
        #             if order is not None:
        #                 trade_update = self._create_trade_update_with_order_fill_data(
        #                     order_fill_msg=data,
        #                     order=order)
        #                 self._order_tracker.process_trade_update(trade_update=trade_update)
        #         elif channel == CONSTANTS.WS_PRIVATE_ORDERS_CHANNEL:
        #             client_order_id = data["clientId"]
        #             order = self._order_tracker.all_updatable_orders.get(client_order_id)
        #             if order is not None:
        #                 order_update = self._create_order_update_with_order_status_data(
        #                     order_status_msg=data,
        #                     order=order)
        #                 self._order_tracker.process_order_update(order_update=order_update)

        #     except asyncio.CancelledError:
        #         raise
        #     except Exception:
        #         self.logger().error(
        #             "Unexpected error in user stream listener loop.", exc_info=True)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
            Example:
            {
                "bch_krw": {
                    "timestamp": 1559285555322,
                    "last": "513000",
                    "open": "523900",
                    "bid": "512100",
                    "ask": "513350",
                    "low": "476200",
                    "high": "540900",
                    "volume": "4477.20611753",
                    "change": "-10900",
                    "changePercent": "-2.08"
                },
                "fet_krw": {
                    "timestamp": 1559285607258,
                    ...
                },
                ...
            }
            """
        retval = []
        for pair in exchange_info_dict:
            try:
                last_price = Decimal(str(exchange_info_dict[pair].get("last")))
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=pair)
                min_order_value = Decimal("5001")
                price_increment = Decimal(str(korbit_utils.get_price_increments(last_price)))
                min_trade_size = min_order_value / last_price * Decimal("1.01")
                size_increment = Decimal("1") / Decimal(str(math.pow(10, 8)))
                min_quote_amount_increment = price_increment * size_increment

                retval.append(TradingRule(trading_pair,
                                          min_order_size=min_trade_size,
                                          min_price_increment=price_increment,
                                          min_base_amount_increment=size_increment,
                                          min_quote_amount_increment=min_quote_amount_increment,
                                          min_order_value=min_order_value,
                                          min_notional_size=min_order_value
                                          ))
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {pair}. Skipping.")
        return retval

    async def _update_balances(self):
        msg = await self._api_request(
            path_url=CONSTANTS.KORBIT_BALANCES_PATH,
            is_auth_required=True)

        # if msg.get("success", False):
        #     balances = msg["result"]
        # else:
        #     raise Exception(msg['msg'])
        balances = msg
        self._account_available_balances.clear()
        self._account_balances.clear()

        for asset in balances:
            balance = balances[asset]
            self._account_balances[asset.upper()] = Decimal(str(balance["available"])) + Decimal(str(balance["trade_in_use"])) + Decimal(str(balance["withdrawal_in_use"]))
            self._account_available_balances[asset.upper()] = Decimal(str(balance["available"]))

    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        # for order in orders:
        try:
            trade_updates = await self._all_trade_updates_for_order(orders=orders)
            for trade_update in trade_updates:
                self._order_tracker.process_trade_update(trade_update)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Failed to fetch trade updates for orders. Error: {request_error}")

    async def _all_trade_updates_for_order(self, orders: List[InFlightOrder]) -> List[TradeUpdate]:
        trade_updates = []
        order_ids = {}
        orders_dict = {}

        for order in orders:
            if order.trading_pair not in order_ids:
                order_ids[order.trading_pair] = []

            exchange_order_id = await order.get_exchange_order_id()
            order_ids[order.trading_pair].append(exchange_order_id)
            orders_dict[exchange_order_id] = order

        try:
            # self.logger().info(f"Trades {order_ids}")
            all_fills_response = []
            for trading_pair in order_ids:
                [('key', 'value1'), ('key', 'value2')]
                params = [("currency_pair", await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair))]
                params += [("id", order_id) for order_id in order_ids[trading_pair]]
                # params = {
                #     "currency_pair": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
                #     "id": order_ids[trading_pair]
                # }
                all_fills_response += await self._api_get(
                    path_url=CONSTANTS.KORBIT_ORDERS_PATH,
                    params=params,
                    is_auth_required=True,
                    limit_id=CONSTANTS.KORBIT_ORDERS_PATH)

                # if len(all_fills_response) > 0:
                    # self.logger().info(f"Trades Resp {all_fills_response}")
        except Exception as e:
            raise IOError(f"Error requesting orders fill updates from Korbit {e}")

        try:
            for trade_fill in all_fills_response:
                if trade_fill["status"] in ["partially_filled", "filled"]:
                    order = orders_dict[trade_fill["id"]]
                    trade_update = self._create_trade_update_with_order_fill_data(order_fill_msg=trade_fill, order=order)
                    if trade_update is not None:
                        trade_updates.append(trade_update)

        except Exception as e:
            raise IOError(f"Error creating trade updates for from fill data {e}")

        return trade_updates

    # async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
    #     trade_updates = []

    #     try:
    #         params = {
    #             "currency_pair": await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair),
    #             "id": await order.get_exchange_order_id()
    #         }
    #         all_fills_response = await self._api_get(
    #             path_url=CONSTANTS.KORBIT_ORDERS_PATH,
    #             params=params,
    #             is_auth_required=True,
    #             limit_id=CONSTANTS.KORBIT_ORDERS_PATH)

    #         self.logger().info(f"Order {order.client_order_id}-{all_fills_response})")

    #         for trade_fill in all_fills_response:
    #             if trade_fill["status"] in ["partially_filled", "filled"]:
    #                 trade_update = self._create_trade_update_with_order_fill_data(order_fill_msg=trade_fill, order=order)
    #                 if trade_update is not None:
    #                     trade_updates.append(trade_update)

    #     except asyncio.TimeoutError:
    #         raise IOError(f"Skipped order update with order fills for {order.client_order_id} "
    #                       "- waiting for exchange order id.")

    #     return trade_updates

    def _create_trade_update_with_order_fill_data(self, order_fill_msg: Dict[str, Any], order: InFlightOrder):

        execute_amount_diff = Decimal(str(order_fill_msg['filled_amount'])) - order.executed_amount_base

        # Emit event if executed amount is greater than 0.
        if execute_amount_diff > s_decimal_0:
            estimated_fee_token = order.base_asset if order.trade_type == TradeType.BUY else order.quote_asset
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=order.trade_type,
                percent_token=estimated_fee_token,
                flat_fees=[TokenAmount(
                    amount=Decimal(str(order_fill_msg["fee"])),
                    token=estimated_fee_token
                )]
            )

            execute_price = Decimal(str(order_fill_msg['price'] if order_fill_msg['price'] != "0" else order_fill_msg['avg_price']))
            execute_quote_diff = Decimal(str(order_fill_msg['filled_total'])) - order.executed_amount_quote

            trade_update = TradeUpdate(
                trade_id=str(order_fill_msg["last_filled_at"]),
                client_order_id=order.client_order_id,
                exchange_order_id=str(order_fill_msg.get("id", order.exchange_order_id)),
                trading_pair=order.trading_pair,
                fee=fee,
                fill_base_amount=execute_amount_diff,
                fill_quote_amount=execute_quote_diff,
                fill_price=execute_price,
                fill_timestamp=float(order_fill_msg["last_filled_at"] / 1000),
            )
            return trade_update

        else:
            return None

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        params = {
            "currency_pair": await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair),
            "id": await tracked_order.get_exchange_order_id()
        }

        updated_order_data = await self._api_get(
            path_url=CONSTANTS.KORBIT_ORDERS_PATH,
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.KORBIT_ORDERS_PATH)

        order_update = self._create_order_update_with_order_status_data(
            order_status_msg=updated_order_data,
            order=tracked_order)

        return order_update

    def _create_order_update_with_order_status_data(self, order_status_msg: Dict[str, Any], order: InFlightOrder):
        state = order.current_state

        if len(order_status_msg) > 0:
            order_status_msg = order_status_msg[0]
            msg_status = order_status_msg["status"]
            if msg_status == "unfilled":
                state = OrderState.OPEN
            elif msg_status == "partially_filled" and (Decimal(str(order_status_msg["filled_total"])) > s_decimal_0):
                state = OrderState.PARTIALLY_FILLED
            elif msg_status == "filled":
                state = OrderState.FILLED
        else:
            state = OrderState.CANCELED

        order_update = OrderUpdate(
            trading_pair=order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=state,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id
        )
        return order_update

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return KorbitAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return KorbitAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol in exchange_info:
            base_currency, quote_currency = symbol.split("_")
            mapping[symbol] = combine_to_hb_trading_pair(base=base_currency.upper(), quote=quote_currency.upper())
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "currency_pair": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_get(
            path_url=CONSTANTS.KORBIT_SINGLE_MARKET_PATH,
            params=params
        )

        return float(resp_json["last"])

