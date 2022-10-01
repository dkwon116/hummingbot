from collections import (
    defaultdict,
    deque
)
from decimal import Decimal
import requests
import datetime, json
import logging
from math import (
    floor,
    ceil
)
from numpy import isnan
import numpy as np
import pandas as pd

from typing import (
    List,
    Tuple,
    Optional
)
from hummingbot.core.clock cimport Clock
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.event.events import BuyOrderCompletedEvent, SellOrderCompletedEvent
from hummingbot.core.data_type.common import TradeType, PriceType, OrderType, PositionAction, PositionMode

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from hummingbot.strategy.strategy_base import StrategyBase
from .perp_xemm_market_pair import CrossExchangeMarketPair
from .order_id_market_pair_tracker import OrderIDMarketPairTracker
from .perp_xemm_entry_status import PerpXEMMEntryStatus
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.client.performance import PerformanceMetrics
from hummingbot.strategy.order_book_asset_price_delegate import OrderBookAssetPriceDelegate
from hummingbot.strategy.asset_price_delegate import AssetPriceDelegate

from ..__utils__.trailing_indicators.exponential_moving_average import ExponentialMovingAverageIndicator

NaN = float("nan")
s_decimal_zero = Decimal("0")
s_decimal_one = Decimal("1")
s_decimal_nan = Decimal("nan")
s_logger = None


cdef class PerpXEMMStrategy(StrategyBase):
    OPTION_LOG_NULL_ORDER_SIZE = 1 << 0
    OPTION_LOG_REMOVING_ORDER = 1 << 1
    OPTION_LOG_ADJUST_ORDER = 1 << 2
    OPTION_LOG_CREATE_ORDER = 1 << 3
    OPTION_LOG_MAKER_ORDER_FILLED = 1 << 4
    OPTION_LOG_STATUS_REPORT = 1 << 5
    OPTION_LOG_MAKER_ORDER_HEDGED = 1 << 6
    OPTION_LOG_ALL = 0x7fffffffffffffff

    ORDER_ADJUST_SAMPLE_INTERVAL = 5
    ORDER_ADJUST_SAMPLE_WINDOW = 12

    SHADOW_MAKER_ORDER_KEEP_ALIVE_DURATION = 60.0 * 15
    CANCEL_EXPIRY_DURATION = 60.0

    @classmethod
    def logger(cls):
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def init_params(self,
                    market_pairs: List[CrossExchangeMarketPair],
                    min_profitability: Decimal,
                    order_amount: Optional[Decimal] = Decimal("0.0"),
                    order_size_taker_volume_factor: Decimal = Decimal("0.9999"),
                    order_size_taker_balance_factor: Decimal = Decimal("0.9999"),
                    order_size_portfolio_ratio_limit: Decimal = Decimal("0.9999"),
                    limit_order_min_expiration: float = 130.0,
                    adjust_order_enabled: bool = True,
                    anti_hysteresis_duration: float = 15.0,
                    active_order_canceling: bint = True,
                    cancel_order_threshold: Decimal = Decimal("0.05"),
                    top_depth_tolerance: Decimal = Decimal(0),
                    logging_options: int = OPTION_LOG_ALL,
                    status_report_interval: float = 900,
                    use_oracle_conversion_rate: bool = False,
                    taker_to_maker_base_conversion_rate: Decimal = Decimal("1"),
                    taker_to_maker_quote_conversion_rate: Decimal = Decimal("1"),
                    slippage_buffer: Decimal = Decimal("0.05"),
                    hb_app_notification: bool = False,
                    enable_reg_offset: bool = False,
                    fixed_beta: Decimal = Decimal("1215"),
                    perp_leverage: int = 1,
                    is_coin_marginated: bool = False,

                    disparity_sensitivity: Decimal = Decimal("0.006"),
                    disparity_factor: Decimal = Decimal("0.5"),
                    std_factor: Decimal = Decimal("0.04"),
                    trend_factor: Decimal = Decimal("10.9"),

                    ema_length: int = 205,
                    trend_sma_length: int = 200,
                    std_length: int = 8,
                    sampling_interval: int = 5,
                    trend_interval: int = 60,
                    # initial_ema: Decimal = Decimal("1230"),

                    asset_price_delegate: AssetPriceDelegate = None,
                    delegate_for: str = '',

                    sell_quote_threshold: Decimal = Decimal("0.5"),
                    sell_adj_factor: Decimal = Decimal("0.5"),

                    sell_profit_factor: Decimal = Decimal("1.0"),
                    buy_profit_factor: Decimal = Decimal("1.0"),
                    incremental_buy_factor: Decimal = Decimal("0.0"),

                    bot_id: str = 'bot_id',
                    reset_balance_when_initializing: bool = True,
                    use_min_profit: bool = True,
                    use_within_range: bool = True,
                    is_grid: bool = False,

                    bo_size: tuple = (Decimal("10.0"), Decimal("5.0")),
                    so_size: tuple = (Decimal("10.0"), Decimal("10.0")),
                    so_levels: tuple = (5, 5),
                    so_volume_factor: tuple = (Decimal("1.5"), Decimal("1.5")),
                    so_initial_step: tuple = (Decimal("0.7"), Decimal("0.7")),
                    so_step_scale: tuple = (Decimal("1.0"), Decimal("1.0")),

                    use_current_entry_status: bool = False,
                    initial_maker_entry_quote: Decimal = Decimal("0.0"),
                    initial_maker_entry_base: Decimal = Decimal("0.0"),
                    initial_taker_entry_quote: Decimal = Decimal("0.0"),
                    initial_taker_entry_base: Decimal = Decimal("0.0"),
                    initial_bo_ratio: Decimal = Decimal("0.0"),
                    ):
        """
        Initializes a cross exchange market making strategy object.

        :param market_pairs: list of cross exchange market pairs
        :param min_profitability: minimum profitability ratio threshold, for actively cancelling unprofitable orders
        :param order_amount: override the limit order trade size, in base asset unit
        :param order_size_taker_volume_factor: maximum size limit of new limit orders, in terms of ratio of hedge-able
                                               volume on taker side
        :param order_size_taker_balance_factor: maximum size limit of new limit orders, in terms of ratio of asset
                                                balance available for hedging trade on taker side
        :param order_size_portfolio_ratio_limit: maximum size limit of new limit orders, in terms of ratio of total
                                                 portfolio value on both maker and taker markets
        :param limit_order_min_expiration: amount of time after which limit order will expire to be used alongside
                                           cancel_order_threshold
        :param cancel_order_threshold: if active order cancellation is disabled, the hedging loss ratio required for the
                                       strategy to force an order cancellation
        :param active_order_canceling: True if active order cancellation is enabled, False if disabled
        :param anti_hysteresis_duration: the minimum amount of time interval between adjusting limit order prices
        :param logging_options: bit field for what types of logging to enable in this strategy object
        :param status_report_interval: what is the time interval between outputting new network warnings
        :param slippage_buffer: Buffer added to the price to account for slippage for taker orders
        """
        if len(market_pairs) < 0:
            raise ValueError(f"market_pairs must not be empty.")
        if not 0 <= order_size_taker_volume_factor <= 1:
            raise ValueError(f"order_size_taker_volume_factor must be between 0 and 1.")
        if not 0 <= order_size_taker_balance_factor <= 1:
            raise ValueError(f"order_size_taker_balance_factor must be between 0 and 1.")

        self._market_pairs = {
            (market_pair.maker.market, market_pair.maker.trading_pair): market_pair
            for market_pair in market_pairs
        }
        self._maker_markets = set([market_pair.maker.market for market_pair in market_pairs])
        self._taker_markets = set([market_pair.taker.market for market_pair in market_pairs])
        self._all_markets_ready = False
        self._min_profitability = min_profitability
        self._order_size_taker_volume_factor = order_size_taker_volume_factor
        self._order_size_taker_balance_factor = order_size_taker_balance_factor
        self._order_amount = order_amount
        self._order_size_portfolio_ratio_limit = order_size_portfolio_ratio_limit
        self._top_depth_tolerance = top_depth_tolerance
        self._cancel_order_threshold = cancel_order_threshold
        self._anti_hysteresis_timers = {}
        self._order_fill_buy_events = {}
        self._order_fill_sell_events = {}
        self._suggested_price_samples = {}
        self._active_order_canceling = active_order_canceling
        self._anti_hysteresis_duration = anti_hysteresis_duration
        self._logging_options = <int64_t>logging_options
        self._last_timestamp = 0
        self._limit_order_min_expiration = limit_order_min_expiration
        self._status_report_interval = status_report_interval
        self._market_pair_tracker = OrderIDMarketPairTracker()
        self._adjust_orders_enabled = adjust_order_enabled
        self._use_oracle_conversion_rate = use_oracle_conversion_rate
        self._taker_to_maker_base_conversion_rate = taker_to_maker_base_conversion_rate
        self._taker_to_maker_quote_conversion_rate = taker_to_maker_quote_conversion_rate
        self._slippage_buffer = slippage_buffer
        self._last_conv_rates_logged = 0
        self._hb_app_notification = hb_app_notification

        self._enable_reg_offset = enable_reg_offset
        self._fixed_beta = fixed_beta
        self._perp_leverage = perp_leverage
        self._is_coin_marginated = is_coin_marginated

        self._ema_length = ema_length
        self._trend_sma_length = trend_sma_length
        self._initial_ema = s_decimal_nan
        self._std_length = std_length
        self._sampling_interval = sampling_interval
        self._trend_interval = trend_interval
        self._ema = ExponentialMovingAverageIndicator(sampling_length=ema_length, processing_length=1)
        self._trend_sma = s_decimal_nan
        self._is_uptrend = False
        self._std = ExponentialMovingAverageIndicator(sampling_length=std_length, processing_length=1)

        self._last_price_ratio = 0
        self._disparity = ()
        self._trend = ()
        self._disparity_sensitivity = disparity_sensitivity
        self._disparity_factor = disparity_factor
        self._std_factor = std_factor
        self._trend_factor = trend_factor
        self._buy_profit = min_profitability
        self._sell_profit = min_profitability
        
        self._asset_price_delegate = asset_price_delegate
        self._delegate_for = delegate_for

        self._sell_quote_threshold = sell_quote_threshold
        self._sell_adj_factor = sell_adj_factor
        self._sell_adj_spread = 0

        self._sell_profit_factor = sell_profit_factor
        self._buy_profit_factor = buy_profit_factor
        self._incremental_buy_factor = incremental_buy_factor

        self._entry_status = PerpXEMMEntryStatus()

        self._bo_size = bo_size
        self._so_size = so_size
        self._so_levels = so_levels
        self._so_volume_factor = so_volume_factor
        self._so_initial_step = so_initial_step
        self._so_step_scale = so_step_scale
        self._order_levels = []
        self._last_hedge_placed = 0

        self._use_current_entry_status = use_current_entry_status
        self._initial_maker_entry_quote = initial_maker_entry_quote
        self._initial_maker_entry_base = initial_maker_entry_base
        self._initial_taker_entry_quote = initial_taker_entry_quote
        self._initial_taker_entry_base = initial_taker_entry_base
        self._initial_bo_ratio = initial_bo_ratio
        self._total_asset = s_decimal_zero

        self._bot_id = bot_id
        self._reset_balance_when_initializing = reset_balance_when_initializing
        self._use_min_profit = use_min_profit
        self._use_within_range = use_within_range
        self._is_grid = is_grid
        
        market_pairs = list(self._market_pairs.values())[0]
        market_pairs.taker.market.set_leverage(market_pairs.taker.trading_pair, self._perp_leverage)

        self._maker_order_ids = []
        cdef:
            list all_markets = list(self._maker_markets | self._taker_markets)

        self.c_add_markets(all_markets)

    @property
    def bot_id(self):
        return self._bot_id
    
    @property
    def reset_balance_when_initializing(self):
        return self._reset_balance_when_initializing
    
    @property
    def use_min_profit(self):
        return self._use_min_profit
    
    @use_min_profit.setter
    def use_min_profit(self, value):
        self._use_min_profit = value

    @property
    def use_within_range(self):
        return self._use_within_range
    
    @use_within_range.setter
    def use_within_range(self, value):
        self._use_within_range = value
    
    @property
    def is_grid(self):
        return self._is_grid

    @property
    def market_pairs(self):
        return self._market_pairs

    @property
    def order_amount(self):
        return self._order_amount
    
    @order_amount.setter
    def order_amount(self, value):
        self._order_amount = value

    @property
    def min_profitability(self):
        return self._min_profitability
    
    @min_profitability.setter
    def min_profitability(self, value):
        self._min_profitability = value

    @property
    def active_limit_orders(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return [(ex, order) for ex, order in self._sb_order_tracker.active_limit_orders
                if order.client_order_id in self._maker_order_ids]

    @property
    def cached_limit_orders(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return self._sb_order_tracker.shadow_limit_orders

    @property
    def active_buys(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return [(market, limit_order) for market, limit_order in self.active_limit_orders if limit_order.is_buy]

    @property
    def active_sells(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return [(market, limit_order) for market, limit_order in self.active_limit_orders if not limit_order.is_buy]

    @property
    def market_pair_tracker(self):
        return self._market_pair_tracker

    @property
    def logging_options(self) -> int:
        return self._logging_options

    @property
    def ema_length(self) -> Decimal:
        return self._ema_length

    @ema_length.setter
    def ema_length(self, value):
        self._ema_length = value
    
    @property
    def trend_sma_length(self) -> Decimal:
        return self._trend_sma_length

    @trend_sma_length.setter
    def trend_sma_length(self, value):
        self._trend_sma_length = value

    @property
    def enable_reg_offset(self) -> Decimal:
        return self._enable_reg_offset

    @property
    def fixed_beta(self) -> Decimal:
        return self._fixed_beta

    @fixed_beta.setter
    def fixed_beta(self, value):
        self._fixed_beta = value
    
    @property
    def ema(self):
        return self._ema

    @ema.setter
    def ema(self, indicator: ExponentialMovingAverageIndicator):
        self._ema = indicator
    
    @property
    def trend_sma(self):
        return self._trend_sma

    @property
    def is_uptrend(self):
        return self._is_uptrend

    @property
    def std(self):
        return self._std

    @std.setter
    def std(self, indicator: ExponentialMovingAverageIndicator):
        self._std = indicator
    
    @property
    def initial_ema(self):
        return self._initial_ema
    
    @property
    def initial_trend_sma(self):
        return self._initial_trend_sma

    @property
    def sampling_interval(self):
        return self._sampling_interval

    @property
    def trend_interval(self):
        return self._trend_interval

    @property
    def last_price_ratio(self):
        return self._last_price_ratio

    @property
    def disparity(self):
        return self._disparity
    
    @property
    def trend(self):
        return self._trend
    
    @property
    def disparity_sensitivity(self):
        return self._disparity_sensitivity
    
    @property
    def disparity_factor(self):
        return self._disparity_factor
    
    @property
    def std_factor(self):
        return self._std_factor
    
    @property
    def trend_factor(self):
        return self._trend_factor
    
    @property
    def buy_profit(self):
        return self._buy_profit
    
    @property
    def sell_profit(self):
        return self._sell_profit

    @property
    def sell_quote_threshold(self):
        return self._sell_quote_threshold
    
    @property
    def sell_adj_factor(self):
        return self._sell_adj_factor
    
    @property
    def sell_adj_spread(self):
        return self._sell_adj_spread

    @property
    def asset_price_delegate(self) -> AssetPriceDelegate:
        return self._asset_price_delegate

    @asset_price_delegate.setter
    def asset_price_delegate(self, value):
        self._asset_price_delegate = value
    
    @property
    def delegate_for(self):
        return self._delegate_for
    
    @property
    def sell_profit_factor(self):
        return self._sell_profit_factor
    
    @sell_profit_factor.setter
    def sell_profit_factor(self, value):
        self._sell_profit_factor = value
    
    @property
    def buy_profit_factor(self):
        return self._buy_profit_factor
    
    @buy_profit_factor.setter
    def buy_profit_factor(self, value):
        self._buy_profit_factor = value
    
    @property
    def incremental_buy_factor(self):
        return self._incremental_buy_factor
    
    @incremental_buy_factor.setter
    def incremental_buy_factor(self, value):
        self._incremental_buy_factor = value

    @property
    def entry_status(self):
        return self._entry_status

    @property
    def bo_size(self):
        return self._bo_size[1] if self._is_uptrend else self._bo_size[0]
    
    @property
    def so_size(self):
        return self._so_size[1] if self._is_uptrend else self._so_size[0]
    
    @property
    def so_levels(self):
        return self._so_levels[1] if self._is_uptrend else self._so_levels[0]
    
    @property
    def so_volume_factor(self):
        return self._so_volume_factor[1] if self._is_uptrend else self._so_volume_factor[0]
    
    @property
    def so_initial_step(self):
        return self._so_initial_step[1] if self._is_uptrend else self._so_initial_step[0]
    
    @property
    def so_step_scale(self):
        return self._so_step_scale[1] if self._is_uptrend else self._so_step_scale[0]
    
    @property
    def order_levels(self):
        return self._order_levels
    
    @property
    def last_hedge_placed(self):
        return self._last_hedge_placed
    
    @property
    def total_asset(self):
        return self._total_asset
    
    @total_asset.setter
    def total_asset(self, value):
        self._total_asset = value

    @logging_options.setter
    def logging_options(self, int64_t logging_options):
        self._logging_options = logging_options
    
    @property
    def perp_positions(self):
        market_pair = list(self._market_pairs.values())[0]
        taker_market = market_pair.taker
        return [s for s in taker_market.market.account_positions.values() if
                s.trading_pair == taker_market.trading_pair and s.amount != s_decimal_zero]

    def get_ref_bid_ask_price(self):
        price_provider = self._asset_price_delegate

        bid = price_provider.get_price_by_type(PriceType.BestBid)
        ask = price_provider.get_price_by_type(PriceType.BestAsk)

        return bid, ask
    
    def is_uptrend(self):
        return Decimal(str(self._ema.current_value)) * (1 + self._sell_profit) > self._trend_sma

    def get_position_in_base_amount(self):
        market_pair = list(self._market_pairs.values())[0]
        taker = market_pair.taker
        if self._is_coin_marginated:
            amount = taker.market.get_value_from_contract(taker.trading_pair, self.perp_positions[0].amount) / self.perp_positions[0].entry_price
        return amount if self._is_coin_marginated else abs(self.perp_positions[0].amount)

    def get_taker_maker_conversion_rate(self) -> Tuple[str, Decimal, str, Decimal]:
        """
        Find conversion rates from taker market to maker market
        :return: A tuple of quote pair symbol, quote conversion rate source, quote conversion rate,
        base pair symbol, base conversion rate source, base conversion rate
        """
        quote_rate = Decimal("1")
        market_pairs = list(self._market_pairs.values())[0]
        quote_pair = f"{market_pairs.taker.quote_asset}-{market_pairs.maker.quote_asset}"  #USD-KRW 1193
        quote_rate_source = "fixed"
        if self._use_oracle_conversion_rate:
            if market_pairs.taker.quote_asset != market_pairs.maker.quote_asset:
                quote_rate_source = RateOracle.source.name
                quote_rate = RateOracle.get_instance().rate(quote_pair)
        else:
            quote_rate = self._taker_to_maker_quote_conversion_rate
        base_rate = Decimal("1")
        base_pair = f"{market_pairs.taker.base_asset}-{market_pairs.maker.base_asset}"
        base_rate_source = "fixed"
        if self._use_oracle_conversion_rate:
            if market_pairs.taker.base_asset != market_pairs.maker.base_asset:
                base_rate_source = RateOracle.source.name
                base_rate = RateOracle.get_instance().rate(base_pair)
        else:
            base_rate = self._taker_to_maker_base_conversion_rate
        return quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate

    def get_total_assets(self, market_pair):
        unreal_pnl = 0
        maker = market_pair.maker
        taker = market_pair.taker
        maker_bid, maker_ask, taker_bid, taker_ask = self.c_get_bid_ask_prices(market_pair)
        maker_mid = maker.get_mid_price()
        taker_mid = taker.get_mid_price()

        total_base = maker.base_balance + taker.base_balance
        # total_base_from_quote = maker.quote_balance / maker_mid + taker.quote_balance / taker_mid
        total_quote_from_base = (maker.base_balance + taker.base_balance) * taker_ask
        taker_asset_in_base = taker.base_balance + taker.quote_balance / taker_mid

        total_base_from_quote = maker.quote_balance / maker_bid + taker.quote_balance / taker_ask
        # total_quote_from_base = (maker.base_balance + taker.base_balance) * taker_ask

        if len(self.perp_positions) > 0:
            pos = self.perp_positions[0]
            if self._is_coin_marginated:
                cont_value = taker.market.get_value_from_contract(taker.trading_pair, pos.amount)
                # unreal_pnl = (cont_value / pos.entry_price) - (cont_value / taker_mid)
                unreal_pnl = (cont_value / pos.entry_price) - (cont_value / taker_bid)
            else:
                unreal_pnl = (pos.amount * (taker_ask - pos.entry_price)) / taker_ask
            taker_asset_in_base += unreal_pnl

        total_asset_in_base = total_base + total_base_from_quote + unreal_pnl
        total_asset_in_quote = total_quote_from_base + taker.quote_balance + (maker.quote_balance / maker_bid * taker_ask) + (unreal_pnl * taker_ask)

        return taker_asset_in_base, total_asset_in_base, total_asset_in_quote

    def get_maker_entry_ratio(self, market_pair):
        maker = market_pair.maker
        maker_mid = maker.get_mid_price()
        maker_balance = maker.quote_balance + maker.base_balance * maker_mid
        return maker.quote_balance / maker_balance

    def get_effective_leverage(self, market_pair):
        taker = market_pair.taker

        taker_mid = taker.get_mid_price()
        taker_total_value_in_quote = (taker.base_balance * taker_mid) + taker.quote_balance
        taker_asset_in_base, _, _ = self.get_total_assets(market_pair)

        perp_amount = self.perp_positions[0].amount if len(self.perp_positions) > 0 else 0

        if self._is_coin_marginated:
            perp_value = taker.market.get_value_from_contract(taker.trading_pair, perp_amount)
        else:
            perp_value = -perp_amount * taker_mid

        return perp_value / (taker_asset_in_base * taker_mid)

    def log_conversion_rates(self, market_pair):
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_taker_maker_conversion_rate()
        maker_bid, maker_ask, taker_bid, taker_ask = self.c_get_bid_ask_prices(market_pair)

        if quote_pair.split("-")[0] != quote_pair.split("-")[1]:
            self.logger().info(f"[Rate] {quote_pair}-[conversion]-{PerformanceMetrics.smart_round(quote_rate)}-{self._ema.current_value:.2f}-[maker]-{maker_bid:.8f}-{maker_ask:.8f}-[taker]-{taker_bid:.8f}-{taker_ask:.8f}")
        if base_pair.split("-")[0] != base_pair.split("-")[1]:
            self.logger().info(f"[Rate] {base_pair}-[conversion]-{PerformanceMetrics.smart_round(base_rate)}-{self._ema.current_value:.2f}-[maker]-{maker_bid:.8f}-{maker_ask:.8f}-[taker]-{taker_bid:.8f}-{taker_ask:.8f}")

    def get_status_dict(self, market_pair):
        # - Asset Information
        # - Entry Status
        # - Current Premium
        # - Position Information

        maker = market_pair.maker
        taker = market_pair.taker

        status = {
            "bot_id": self._bot_id,
            "updated_at": datetime.datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        }

        market_df = self.market_status_data_frame([maker, taker])
        asset_df = self.wallet_balance_data_frame([maker, taker]).to_dict("records")
        entry_df = self.entry_df().to_dict("records")
        premium_df = self.premium_status_df().to_dict("records")
        position_df = self.active_positions_df().to_dict("records")

        status["market"] = market_df.to_dict("records")
        status["assets"] = asset_df
        status["entry"] = entry_df
        status["premium"] = premium_df
        status["positions"] = position_df

        taker_asset_in_base, total_asset_in_base, total_asset_in_quote = self.get_total_assets(market_pair)
        taker_perc = taker_asset_in_base / total_asset_in_base
        total_asset = total_asset_in_base if self._is_coin_marginated else total_asset_in_quote

        maker_last, taker_last = market_df["Last"]
        status["asset_summary"] = {
            "total": float(total_asset),
            "taker_perc": float(taker_perc),
            "conversion": float(maker_last / taker_last)
        }

        status["entry_summary"] = {
            "ema": self._ema.current_value,
            "quote_ratio": float(self.get_maker_entry_ratio(market_pair)),
            "entry": float(self._entry_status.average_ratio),
            "min_tp": float(self.get_min_profit_ratio()),
            "maker_base_sold": self._entry_status.get_entry_value("maker", "base")
        }

        return status

    def log_status_to_db(self, market_pair):
        status = self.get_status_dict(market_pair)
        log_url = "https://a9jkrisu3g.execute-api.ap-northeast-2.amazonaws.com/api/update_status"
        rsp = requests.post(url=log_url, json=status)
    
    def get_moving_averages(self, market_pair):
        base_url = "https://a9jkrisu3g.execute-api.ap-northeast-2.amazonaws.com/api/signal"
        maker = market_pair.maker
        taker = market_pair.taker

        # WAVES-KRW_WAVES-BUSD_15_30_163
        pair = f"{maker.trading_pair}_{taker.trading_pair}_{self._sampling_interval}_{self._ema_length}_{self._trend_sma_length}"
        maker = f"{maker.market.name}-{taker.market.name}"
        url = f"{base_url}/{maker}/{pair}"

        rsp = requests.get(url=url)
        data = rsp.json()

        ema = Decimal(data["ema"])
        trend_sma = Decimal(data["trend_sma"])

        return ema, trend_sma

    def oracle_status_df(self):
        columns = ["Source", "Pair", "Rate"]
        data = []
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_taker_maker_conversion_rate()
        if quote_pair.split("-")[0] != quote_pair.split("-")[1]:
            data.extend([
                [quote_rate_source, quote_pair, PerformanceMetrics.smart_round(quote_rate)],
            ])
        if base_pair.split("-")[0] != base_pair.split("-")[1]:
            data.extend([
                [base_rate_source, base_pair, PerformanceMetrics.smart_round(base_rate)],
            ])
        return pd.DataFrame(data=data, columns=columns)

    def premium_status_df(self):
        """
        Fixed show absolute premium 
        EMA adjusted percent does not represent profit, but current center
        """
        market_pair = list(self._market_pairs.values())[0]
        maker_market = market_pair.maker.market
        taker_market = market_pair.taker.market
        tm, mt = self.c_calculate_premium(market_pair, False)
        adj_tm, adj_mt = self.c_calculate_premium(market_pair, True)
        
        columns = [f"{maker_market.display_name}", f"{taker_market.display_name}", "Current", "Ratio", "Target", "Target Ratio", "Price"]
        data = []

        ema = Decimal(self._ema.current_value)
        taker_hedge_sell_price = self.c_calculate_effective_hedging_price(market_pair, True, self._order_amount)
        taker_hedge_buy_price = self.c_calculate_effective_hedging_price(market_pair, False, self._order_amount)

        entry_target = self._sell_profit
        entry_ratio = ema * (1 + self._sell_profit)
        entry_price = taker_hedge_buy_price * (1 + self.get_profitability(market_pair, False))
        tp_target = self._buy_profit
        tp_ratio = ema / (1 + self._buy_profit)
        tp_price = taker_hedge_sell_price / (1 + self.get_profitability(market_pair, True))

        if self._entry_status.current_entry_ratio > s_decimal_zero and self._use_min_profit:
            entry_ratio = max(self._entry_status.current_entry_ratio, entry_ratio) if not self._is_grid else self._entry_status.current_entry_ratio
            entry_target = entry_ratio / ema - 1
            entry_price = (taker_hedge_buy_price / self.market_conversion_rate(True)) * Decimal(entry_ratio)
        
        if self._entry_status.average_ratio > s_decimal_zero and self._use_min_profit:
            tp_ratio = min(self.get_min_profit_ratio(), tp_ratio) if not self._is_grid else self.get_min_profit_ratio()
            tp_target = 1 - tp_ratio / ema
            tp_price = (taker_hedge_sell_price / self.market_conversion_rate(True)) * Decimal(tp_ratio)
        
        data.extend([
            ["Sell", "Buy", f"{adj_mt:.2%}", f"{ema * (1+adj_mt):.2f}", f"{entry_target:.2%}", f"{entry_ratio:.2f}", f"{entry_price:.4f}"],
            ["Buy", "Sell", f"{adj_tm:.2%}", f"{ema / (1+adj_tm):.2f}", f"{tp_target:.2%}", f"{tp_ratio:.2f}", f"{tp_price:.4f}"],
        ])

        return pd.DataFrame(data=data, columns=columns)
    
    def profit_calc_df(self):
        columns = [f"Side", "Profit", "Disparity", "Trend", "Std"]
        data = []
        std = Decimal(str(self._std.std_dev(self._std_length) / self._ema.current_value))
        data.extend([
            ["Sell", f"{self._sell_profit:.2%}", f"{self._disparity[0]:.4%}", f"{self._trend[0]:.4%}", f"{std:.3%}"],
            ["Buy", f"{self._buy_profit:.2%}", f"{self._disparity[1]:.4%}", f"{self._trend[1]:.4%}", f"{std:.3%}"],
        ])

        return pd.DataFrame(data=data, columns=columns)

    def active_positions_df(self) -> pd.DataFrame:
        """
        Returns a new dataframe on current active perpetual positions.
        """
        columns = ["Symbol", "Type", "Entry Price", "Amount", "Leverage", "Unrealized PnL", "Liq Price"]
        data = []
        for pos in self.perp_positions:
            data.append([
                pos.trading_pair,
                "LONG" if pos.amount > 0 else "SHORT",
                float(PerformanceMetrics.smart_round(pos.entry_price)),
                float(pos.amount),
                int(pos.leverage),
                float(PerformanceMetrics.smart_round(pos.unrealized_pnl)),
                float(PerformanceMetrics.smart_round(pos.liquidation_price))
            ])

        return pd.DataFrame(data=data, columns=columns)

    def market_status_data_frame(self, market_trading_pair_tuples) -> pd.DataFrame:
        markets_data = []
        markets_columns = ["Exchange", "Market", "Best Bid", "Best Ask", "Mid", "Last", "Spread"]

        try:
            if type(self._asset_price_delegate) is OrderBookAssetPriceDelegate:
                delegate_market = self._asset_price_delegate.market
                del_tp, del_base, del_quote = delegate_market.trading_pair[self._asset_price_delegate.trading_pair]
                market_trading_pair_tuples.append((delegate_market, del_tp, del_base, del_quote))
            for market_trading_pair_tuple in market_trading_pair_tuples:
                market, trading_pair, base_asset, quote_asset = market_trading_pair_tuple
                bid_price = market.get_price(trading_pair, False)
                ask_price = market.get_price(trading_pair, True)
                spread = (float(ask_price) / float(bid_price) - 1)
                mid_price = (bid_price + ask_price)/2
                last_price = market.get_price_by_type(trading_pair, PriceType.LastTrade)
                markets_data.append([
                    market.display_name,
                    trading_pair,
                    float(bid_price),
                    float(ask_price),
                    float(mid_price),
                    float(last_price),
                    f"{spread:.2%}"
                ])
            return pd.DataFrame(data=markets_data, columns=markets_columns)

        except Exception:
            self.logger().error("Error formatting market stats.", exc_info=True)
    
    def entry_df(self):
        """
        Show what percent of base asset from maker each BO/SO should fill (sell)
        If BO asset percent is filled, then show SO ratio levels
        Check if each BO/SO level is filled
        """
        levels_data = []
        level_columns = ["Amount %", "Cum Amount", "Deviation", "Ratio", "Filled"]

        for order_level in self._order_levels:
            levels_data.append([
                f"{order_level['amount_perc']:.0%}",
                f"{PerformanceMetrics.smart_round(order_level['cum_amount'])}",
                f"{order_level['total_deviation'] - s_decimal_one:.2%}",
                f"{order_level['ratio']:.2f}",
                f"{order_level['filled']:.2%}"
            ])
        return pd.DataFrame(data=levels_data, columns=level_columns)

    def format_status(self) -> str:
        cdef:
            list lines = []
            list warning_lines = []
            dict tracked_maker_orders = {}
            LimitOrder typed_limit_order

        # Go through the currently open limit orders, and group them by market pair.
        for _, limit_order in self.active_limit_orders:
            typed_limit_order = limit_order
            market_pair = self._market_pair_tracker.c_get_market_pair_from_order_id(typed_limit_order.client_order_id)
            if market_pair not in tracked_maker_orders:
                tracked_maker_orders[market_pair] = {typed_limit_order.client_order_id: typed_limit_order}
            else:
                tracked_maker_orders[market_pair][typed_limit_order.client_order_id] = typed_limit_order

        for market_pair in self._market_pairs.values():
            maker = market_pair.maker
            taker = market_pair.taker

            warning_lines.extend(self.network_warning([maker, taker]))

            markets_df = self.market_status_data_frame([maker, taker])
            if self._asset_price_delegate is not None:
                maker_mid, taker_mid, delegate_mid = markets_df["Mid"]
                maker_last, taker_last, delegate_last = markets_df["Last"]
            else:
                maker_mid, taker_mid = markets_df["Mid"]
                maker_last, taker_last = markets_df["Last"]

            spread_ratio = maker_mid / taker_mid
            spread_ratio_last = maker_last / taker_last
            
            if self._asset_price_delegate is not None:
                ref_spread_last = delegate_last / taker_last

                lines.extend(["", f"  Markets: Last M/T Ratio {spread_ratio_last:.2f} Ref Last Ratio {ref_spread_last:.2f}"] +
                            ["    " + line for line in str(markets_df).split("\n")])
            else:
                lines.extend(["", f"  Markets: Last M/T Ratio {spread_ratio_last:.2f}"] +
                            ["    " + line for line in str(markets_df).split("\n")])

            oracle_df = self.oracle_status_df()
            if not oracle_df.empty:
                lines.extend(["", "  Rate conversion:"] +
                             ["    " + line for line in str(oracle_df).split("\n")])


            assets_df = self.wallet_balance_data_frame([maker, taker])
            taker_asset_in_base, total_asset_in_base, total_asset_in_quote = self.get_total_assets(market_pair)

            taker_perc = taker_asset_in_base / total_asset_in_base
            total_asset = total_asset_in_base if self._is_coin_marginated else total_asset_in_quote
            asset = taker.base_asset if self._is_coin_marginated else taker.quote_asset
            profit_perc = (total_asset / self._total_asset) - s_decimal_one
            lines.extend(["", f"  Assets: Total {PerformanceMetrics.smart_round(total_asset)}{asset} Taker {taker_perc:.2%} Profit% {profit_perc:.3%}"] +
                         ["    " + line for line in str(assets_df).split("\n")])
            
            factor_df = self.profit_calc_df()
            if not factor_df.empty:
                lines.extend(["", f"  Profit Factors: Quote Ratio {self.get_maker_entry_ratio(market_pair):.0%}"] +
                             ["    " + line for line in str(factor_df).split("\n")])

            entry_df = self.entry_df()
            maker_base_sold = self._entry_status.get_entry_value("maker", "base")
            trend_is = "Up" if self._is_uptrend else "Down"
            lines.extend(["", f"  Entry: Maker Base Sold {maker_base_sold:.4f} Trend {trend_is}"] +
                         ["    " + line for line in str(entry_df).split("\n")])

            premium_df = self.premium_status_df()
            entry_ratio = self._entry_status.average_ratio
            if not premium_df.empty:
                lines.extend(["", f"  Premium: EMA {self._ema.current_value:.2f} Trend MA {self._trend_sma:.2f} Entry Ratio {entry_ratio:.2f} Min TP {self.get_min_profit_ratio():.2f}"] +
                             ["    " + line for line in str(premium_df).split("\n")])

            # See if there're any active positions.
            if len(self.perp_positions) > 0:
                position_df = self.active_positions_df()
                eff_leverage = self.get_effective_leverage(market_pair)
                lines.extend(["", f"  Positions: Eff Lev {eff_leverage:.2f}x"] + 
                             ["    " + line for line in position_df.to_string(index=False).split("\n")])
            else:
                lines.extend(["", "  No active positions."])


            # See if there're any open orders.
            if market_pair in tracked_maker_orders and len(tracked_maker_orders[market_pair]) > 0:
                limit_orders = list(tracked_maker_orders[market_pair].values())
                bid, ask = self.c_get_top_bid_ask(market_pair)
                mid_price = (bid + ask)/2

                taker_sell = markets_df["Best Bid"][1] * self._ema.current_value
                taker_buy = markets_df["Best Ask"][1] * self._ema.current_value

                df = LimitOrder.to_pandas(limit_orders, mid_price)

                # Add spread to hedge order
                tm_spread = taker_sell / df["Price"]
                mt_spread = df["Price"] / taker_buy
                df["Spread"] = np.where(df["Type"] == "buy", tm_spread, mt_spread)
                df["Spread"] = ((df["Spread"] - 1) * 100).round(2)
                
                df_lines = str(df).split("\n")
                lines.extend(["", "  Active orders:"] +
                             ["    " + line for line in df_lines])
            else:
                lines.extend(["", "  No active maker orders."])

            warning_lines.extend(self.balance_warning([maker, taker]))

        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def initialize_order_levels(self, market_pair):
        # run when maker_base_sold is 0
        maker = market_pair.maker
        taker = market_pair.taker
        
        taker_asset_in_base, total_asset_in_base, total_asset_in_quote = self.get_total_assets(market_pair)
        total_asset = total_asset_in_base if self._is_coin_marginated else total_asset_in_quote
        asset = taker.base_asset if self._is_coin_marginated else taker.quote_asset

        if self._total_asset == s_decimal_zero:
            self._total_asset = total_asset

        self._buy_profit_factor = Decimal("2.0")
        profit = total_asset - self._total_asset
        profit_perc = (total_asset / self._total_asset) - s_decimal_one
        self._total_asset = total_asset

        order_levels_array = []
        available_base = maker.market.get_available_balance(maker.base_asset) + self._entry_status.get_entry_value("maker", "base")
        self.logger().info(f"[Level]-Profit {profit:.4f}{asset}-Profit % {profit_perc:.3%}-Total Assest {total_asset:.4f}{asset}")
        self.logger().info(f"[Level]-Initializing {self.so_levels} Order Levels with {available_base} base asset")
        self.notify_hb_app(f"Completed-Profit {profit:.4f}{asset}-Profit % {profit_perc:.3%}-Total Assest {total_asset:.4f}{asset}")
        self.notify_hb_app(f"Initializing-Total Assest {total_asset:.4f}{asset}-Maker Base {available_base}-Up Trend {self._is_uptrend}")
        
        # (amount in base, base %, cumulative base, so deviation from last, total deviation, ratio_level)
        bo_amount = available_base * self.bo_size
        cumulative_amount = bo_amount
        total_deviation = s_decimal_one

        base_order = {
            "amount": bo_amount,
            "amount_perc": self.bo_size,
            "cum_amount": bo_amount,
            "dev_from_last": s_decimal_zero,
            "total_deviation": total_deviation,
            "ratio": s_decimal_zero,
            "filled": s_decimal_zero
        }
        order_levels_array.append(base_order)

        for i in range(1, self.so_levels + 1):
            so_percent = self.so_size * self.so_volume_factor**(i - 1)
            so_amount = available_base * so_percent
            cumulative_amount += so_amount
            dev_from_last = self.so_initial_step * self.so_step_scale**(i - 1)

            if self._is_coin_marginated:
                if i == self.so_levels:
                    dev_from_last *= Decimal("8")
                elif i == (self.so_levels - 1):
                    dev_from_last *= Decimal("3")

            total_deviation = total_deviation * (1 + dev_from_last)
            safety_order = {
                "amount": so_amount,
                "amount_perc": so_percent,
                "cum_amount": cumulative_amount,
                "dev_from_last": dev_from_last,
                "total_deviation": total_deviation,
                "ratio": s_decimal_zero,
                "filled": s_decimal_zero
            }
            order_levels_array.append(safety_order)
        
        self._order_levels = order_levels_array
        return order_levels_array

    def reset_entry_status_with_current_status(self, market_pair):
        entry_status = self._entry_status
        maker = market_pair.maker
        taker = market_pair.taker

        entry_status.update_entry("maker", self._initial_maker_entry_quote, self._initial_maker_entry_base)
        entry_status.update_entry("taker", self._initial_taker_entry_quote, self._initial_taker_entry_base)

        entry_status.trade_in_progress_active(True)
        entry_status.start_balancing()
        entry_status.start_ratio_update()

    def set_base_order_ratio(self, market_pair):
        self._order_levels[0]["ratio"] = self._entry_status.average_ratio if self._initial_bo_ratio == s_decimal_zero else self._initial_bo_ratio
        self._initial_bo_ratio = s_decimal_zero
        self.logger().info(f"[Level]-Set Base Ratio-{self._entry_status.average_ratio:.2f}")
        self._entry_status.bo_ratio_set()

    def set_safety_order_levels(self, market_pair):
        # set when BO entry is completed
        # should check BO entry percent and taker hedge amount
        base_order = self._order_levels[0]
        base_order_ratio = base_order["ratio"]
        self.logger().info(f"[Level]-Set SO Levels")

        so_levels = len(self._order_levels)
        for i in range(1, so_levels):
            order_level = self._order_levels[i]
            order_level["ratio"] = base_order_ratio * (order_level["total_deviation"])
        
        if len(self._order_levels) > 0:
            self._entry_status.update_entry_level(1, self._order_levels[1]["ratio"])
        else:
            self._entry_status.update_entry_level(1, s_decimal_zero)

    def check_current_level_and_ratio(self, market_pair):
        current_level = 0
        maker_base_sold = self._entry_status.get_entry_value("maker", "base")

        for order in self._order_levels:
            if maker_base_sold > order["cum_amount"]:
               current_level += 1
            else:
                return current_level, order["ratio"] 

    def process_maker_sell_fills(self):
        current_level = 0
        maker_base_sold = self._entry_status.get_entry_value("maker", "base")
        market_pair = list(self._market_pairs.values())[0]

        for order in self._order_levels:
            current_ratio = order["ratio"]
            if maker_base_sold >= order["cum_amount"]:
                current_level += 1
                if order["filled"] != s_decimal_one:
                    order["filled"] = s_decimal_one
            else:
                prev_cumulative_amount = order["cum_amount"] - order["amount"]
                order["filled"] = (maker_base_sold - prev_cumulative_amount) / order["amount"]
                break

        if current_level > self._entry_status.current_entry_level:
            self.notify_hb_app(f"Entry - {self.get_maker_entry_ratio(market_pair):.0%} Entry @ Level {current_level - 1} Filled")

            if current_level >= 1 and self._entry_status.current_entry_level == 0:
                self.logger().info(f"[Level]-BO filled setting order levels")
                self._entry_status.set_order_levels()    
            else:
                self.logger().info(f"[Level]-Increase to Level {current_level:.0f} ")
                self._entry_status.update_entry_level(current_level, current_ratio)    

            # increase level subtract inc factor and get min of factor or base factor (at minimum base)
            self._buy_profit_factor = max(self._buy_profit_factor - self._incremental_buy_factor, Decimal("2.0"))
            self.logger().info(f"[Level]-Update ratio to {current_ratio:.2f} for level {current_level:.0f} TP at {(self._min_profitability * self._buy_profit_factor):.2%}")

        if current_level < self._entry_status.current_entry_level:
            current_ratio = s_decimal_zero if current_level == 0 else current_ratio
            self._entry_status.update_entry_level(current_level, current_ratio)
            self.logger().info(f"[Level]-Decrease to Level {current_level:.0f}")
            level = 0
            for order in self._order_levels:
                if level > current_level:
                    order["filled"] = s_decimal_zero
                level += 1
                # if order["ratio"] > current_ratio:
                #     order["filled"] = s_decimal_zero
            # decrease level add 0.5 to buy factor
            self._buy_profit_factor += self._incremental_buy_factor
            self.notify_hb_app(f"TP - {self.get_maker_entry_ratio(market_pair):.0%} Remaining @ Level {current_level}")
            self.logger().info(f"[Level]-Update ratio to {current_ratio:.2f} for level {current_level:.0f} TP at {(self._min_profitability * self._buy_profit_factor):.2%}")

        return current_level, current_ratio

    def reset_position(self, market_pair):
        taker = market_pair.taker
        taker_position = self.perp_positions[0].amount if len(self.perp_positions) > 0 else s_decimal_zero

        if self._is_coin_marginated:
            balancing_is_buy = False if taker_position > s_decimal_zero else True
            balancing_amount = taker.market.quantize_order_amount(taker.trading_pair, abs(taker_position), taker.get_mid_price())
        else:
            maker_balance = market_pair.maker.base_balance
            balance_diff = maker_balance + taker_position
            balancing_is_buy = True if balance_diff < s_decimal_zero else False
            balancing_amount = taker.market.quantize_order_amount(taker.trading_pair, abs(balance_diff), taker.get_mid_price())

        if balancing_amount > s_decimal_zero:
            taker_price = taker.get_price(balancing_is_buy)
            self.c_place_order(market_pair, balancing_is_buy, False, balancing_amount, taker_price)
            balancing = "Coin" if self._is_coin_marginated else "USDT"
            self.logger().info(f"[Level]-balancer-{balancing} reset balancing amount{balancing_amount}-balancing_is_buy-{balancing_is_buy}")

    def maker_taker_balance_diffs(self, market_pair):
        maker = market_pair.maker
        taker = market_pair.taker
        
        if self._is_coin_marginated:
            maker_entry = self._entry_status.get_entry_value("maker", "base")
            taker_bought = taker.market.get_value_from_contract(taker.trading_pair, self.perp_positions[0].amount) / self.perp_positions[0].entry_price if len(self.perp_positions) > 0 else s_decimal_zero
            base_diff = maker_entry - taker_bought
        else:
            taker_amount = self.perp_positions[0].amount if len(self.perp_positions) > 0 else s_decimal_zero
            base_diff = -(maker.base_balance + taker_amount)

        taker_diff_value = base_diff * taker.get_mid_price()
        return base_diff, taker_diff_value

    def maker_taker_balancing_needed(self, market_pair):
        taker = market_pair.taker
        balance_tolerance_value = taker.market.get_value_from_contract(taker.trading_pair, s_decimal_one) if self._is_coin_marginated else Decimal("6")
        base_diff, taker_diff_value = self.maker_taker_balance_diffs(market_pair)
        quantized_maker = market_pair.maker.market.quantize_order_amount(market_pair.maker.trading_pair, abs(base_diff))
        quantized_taker = market_pair.taker.market.quantize_order_amount(market_pair.taker.trading_pair, abs(base_diff))

        balancing_needed = abs(taker_diff_value) > balance_tolerance_value and quantized_maker > s_decimal_zero and quantized_taker > s_decimal_zero

        if balancing_needed:
            self.logger().info(f"[Level]-balancer-maker taker diff value {taker_diff_value:.0f}")

        return balancing_needed
        
    def balance_maker_taker_positions(self, market_pair):
        # run after each level is filled
        # coin: sell amount == cont_value / entry_price
        # tether: sell amount == abs(position_amount)
        if self.maker_taker_balancing_needed(market_pair):
            taker = market_pair.taker
            base_diff, taker_diff_value = self.maker_taker_balance_diffs(market_pair)
            balancing_is_buy = taker_diff_value > s_decimal_zero

            if self._is_coin_marginated:
                balancing_amount = Decimal(taker.market.get_contract_amount_from_value(taker.trading_pair, abs(taker_diff_value)))
            else:
                balancing_amount = taker.market.quantize_order_amount(taker.trading_pair, abs(base_diff), taker.get_mid_price())
            
            taker_price = market_pair.taker.get_price(balancing_is_buy)
            self.c_place_order(market_pair, balancing_is_buy, False, balancing_amount, taker_price)
            self._last_hedge_placed = self._current_timestamp
            self.logger().info(f"[Level]-balancer-position-{balancing_amount}-balancing_is_buy-{balancing_is_buy} at {self._last_hedge_placed}")
        else:
            self._entry_status.balancing_done()

    # The following exposed Python functions are meant for unit tests
    # ---------------------------------------------------------------

    def get_order_size_after_portfolio_ratio_limit(self, market_pair: CrossExchangeMarketPair) -> Decimal:
        return self.c_get_order_size_after_portfolio_ratio_limit(market_pair)
    
    def get_top_bid_ask_from_price_samples(self, market_pair: CrossExchangeMarketPair):
        return self.c_get_top_bid_ask_from_price_samples(market_pair)
    
    def get_bid_ask(self, market_pair: CrossExchangeMarketPair):
        return self.c_get_bid_ask_prices(market_pair)

    def get_market_making_size(self, market_pair: CrossExchangeMarketPair, bint is_bid) -> Decimal:
        return self.c_get_market_making_size(market_pair, is_bid)

    def get_market_making_price(self, market_pair: CrossExchangeMarketPair, bint is_bid, size: Decimal) -> Decimal:
        return self.c_get_market_making_price(market_pair, is_bid, size)

    def get_adjusted_limit_order_size(self, market_pair: CrossExchangeMarketPair, bint is_bid) -> Tuple[Decimal, Decimal]:
        return self.c_get_adjusted_limit_order_size(market_pair, is_bid)

    def get_effective_hedging_price(self, market_pair: CrossExchangeMarketPair, bint is_bid, size: Decimal) -> Decimal:
        return self.c_calculate_effective_hedging_price(market_pair, is_bid, size)

    def check_if_still_profitable(self, market_pair: CrossExchangeMarketPair,
                                  LimitOrder active_order,
                                  current_hedging_price: Decimal) -> bool:
        return self.c_check_if_still_profitable(market_pair, active_order, current_hedging_price)

    def check_if_sufficient_balance(self, market_pair: CrossExchangeMarketPair,
                                    LimitOrder active_order) -> bool:
        return self.c_check_if_sufficient_balance(market_pair, active_order)
    
    def place_order(self, market_pair: CrossExchangeMarketPair, bint is_buy, bint is_maker, amount: Decimal, price: Decimal):
        return self.c_place_order(market_pair, is_buy, is_maker, amount, price)

    # ---------------------------------------------------------------

    cdef c_start(self, Clock clock, double timestamp):
        StrategyBase.c_start(self, clock, timestamp)
        self._last_timestamp = timestamp

    cdef c_stop(self, Clock clock):
        self._initial_maker_entry_base = self._entry_status.get_entry_value("maker", "base")
        self._initial_maker_entry_quote = self._entry_status.get_entry_value("maker", "quote")
        self._initial_taker_entry_base = self._entry_status.get_entry_value("taker", "base")
        self._initial_taker_entry_quote = self._entry_status.get_entry_value("taker", "quote")
        self._initial_bo_ratio = self._order_levels[0]["ratio"]

        StrategyBase.c_stop(self, clock)

    cdef c_tick(self, double timestamp):
        """
        Clock tick entry point.

        For cross exchange market making strategy, this function mostly just checks the readiness and connection
        status of markets, and then delegates the processing of each market pair to c_process_market_pair().

        :param timestamp: current tick timestamp
        """
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
            list active_limit_orders = self.active_limit_orders
            LimitOrder limit_order

        try:
            # Perform clock tick with the market pair tracker.
            self._market_pair_tracker.c_tick(timestamp)

            if not self._all_markets_ready:
                self._all_markets_ready = all([market.ready for market in self._sb_markets])
                if self._asset_price_delegate is not None and self._all_markets_ready:
                    self._all_markets_ready = self._asset_price_delegate.ready
                if not self._all_markets_ready:
                    # Markets not ready yet. Don't do anything.
                    if should_report_warnings:
                        self.logger().warning(f"Markets are not ready. No market making trades are permitted.")
                    return
                else:
                    # Markets are ready, ok to proceed.
                    if self.OPTION_LOG_STATUS_REPORT:
                        self.logger().info(f"Markets are ready. Trading started.")

            if should_report_warnings:
                # Check if all markets are still connected or not. If not, log a warning.
                if not all([market.network_status is NetworkStatus.CONNECTED for market in self._sb_markets]):
                    self.logger().warning(f"WARNING: Some markets are not connected or are down at the moment. Market "
                                          f"making may be dangerous when markets or networks are unstable.")
                    return

            # Calculate a mapping from market pair to list of active limit orders on the market.
            market_pair_to_active_orders = defaultdict(list)

            for maker_market, limit_order in active_limit_orders:
                market_pair = self._market_pairs.get((maker_market, limit_order.trading_pair))
                if market_pair is None:
                    self.log_with_clock(logging.WARNING,
                                        f"The in-flight maker order in for the trading pair '{limit_order.trading_pair}' "
                                        f"does not correspond to any whitelisted trading pairs. Skipping.")
                    continue

                if not self._sb_order_tracker.c_has_in_flight_cancel(limit_order.client_order_id) and \
                        limit_order.client_order_id in self._maker_order_ids:
                    market_pair_to_active_orders[market_pair].append(limit_order)

            # Process each market pair independently.
            for market_pair in self._market_pairs.values():
                self.c_process_market_pair(market_pair, market_pair_to_active_orders[market_pair])
            # log conversion rates every 5 minutes
                if self._last_conv_rates_logged + (60. * 5) < self._current_timestamp:
                    self.log_conversion_rates(market_pair)
                    self._last_conv_rates_logged = self._current_timestamp
                if self._current_timestamp % (60. * 30) == 0:
                    self.log_status_to_db(market_pair)
                
        finally:
            self._last_timestamp = timestamp

    def has_active_taker_order(self, object market_pair):
        cdef dict market_orders = self._sb_order_tracker.c_get_market_orders()
        if len(market_orders.get(market_pair, {})) > 0:
            return True
        cdef dict limit_orders = self._sb_order_tracker.c_get_limit_orders()
        limit_orders = limit_orders.get(market_pair, {})
        if len(limit_orders) > 0:
            if len(set(limit_orders.keys()).intersection(set(self._maker_order_ids))) > 0:
                return True
        return False

    def fill_sampling_buffer(self):
        self.logger().info(f"Filling sampling buffer EMA filled {self._ema.sampling_length_filled()} Trend SMA {self._trend_sma}")
        ema_to_fill = self._ema_length - self._ema.sampling_length_filled()
        std_to_fill = self._std_length - self._std.sampling_length_filled()
        for i in range(ema_to_fill): self._ema.add_sample(self._initial_ema)
        for i in range(std_to_fill): self._std.add_sample(self._initial_ema)
    
    def get_last_trade_ratio(self, market_pair):
        maker_last = market_pair.maker.get_price_by_type(PriceType.LastTrade)
        taker_last = market_pair.taker.get_price_by_type(PriceType.LastTrade)

        return maker_last / taker_last

    cdef c_process_market_pair(self, object market_pair, list active_orders):
        """
        For market pair being managed by this strategy object, do the following:

         1. Check whether any of the existing orders need to be cancelled.
         2. Check if new orders should be created.

        For each market pair, only 1 active bid offer and 1 active ask offer is allowed at a time at maximum.

        If an active order is determined to be not needed at step 1, it would cancel the order within step 1.

        If there's no active order found in step 1, and condition allows (i.e. profitability, account balance, etc.),
        then a new limit order would be created at step 2.

        Combining step 1 and step 2 over time, means the offers made to the maker market side would be adjusted over
        time regularly.

        :param market_pair: cross exchange market pair
        :param active_orders: list of active limit orders associated with the market pair
        """
        cdef:
            object current_hedging_price
            ExchangeBase taker_market
            bint is_buy
            bint has_active_buy = False
            bint has_active_sell = False
            bint need_adjust_order = False
            double anti_hysteresis_timer = self._anti_hysteresis_timers.get(market_pair, 0)

        global s_decimal_zero

        if self._last_conv_rates_logged == 0:
            self._initial_ema, self._trend_sma = self.get_moving_averages(market_pair)
            self.logger().info(f"Initializing with {self._initial_ema} EMA and {self._trend_sma} Trend SMA")

            self.fill_sampling_buffer()
            if self._use_current_entry_status:
                self.reset_entry_status_with_current_status(market_pair)
            
            self._is_uptrend = (Decimal(str(self._ema.current_value)) * (1 + self._sell_profit)) > self._trend_sma
            self.initialize_order_levels(market_pair)
            
            if self._use_current_entry_status:
                self.process_maker_sell_fills()
                self._use_current_entry_status = False

        if  (self._current_timestamp % self._sampling_interval) == 0:
            m_bid, m_ask, t_bid, t_ask = self.c_get_bid_ask_prices(market_pair)
            
            if self._asset_price_delegate is not None:
                if self._delegate_for == "maker":
                    m_bid, m_ask = self.get_ref_bid_ask_price()
                elif self._delegate_for == "taker":
                    t_bid, t_ask = self.get_ref_bid_ask_price()

            last_ratio = self.get_last_trade_ratio(market_pair)

            if self._ema.sampling_length_filled() > 0:
                self._ema.add_sample(last_ratio)
                self._std.add_sample(last_ratio)
        
            if (self._current_timestamp % self._trend_interval) == 0:
                ema, trend_sma = self.get_moving_averages(market_pair)
                new_trend = trend_sma
                self.logger().info(f"[Rate]-trend SMA changed from {self._trend_sma} to {new_trend} Is Uptrend {self.is_uptrend()}")
                self._trend_sma = new_trend

            updated_is_uptrend = (Decimal(str(self._ema.current_value)) * (1 + self._sell_profit)) > self._trend_sma
            
            if self._is_uptrend != updated_is_uptrend:
                self._is_uptrend = updated_is_uptrend
                if self._entry_status.average_ratio == s_decimal_zero:
                    self.initialize_order_levels(market_pair)

        self.c_take_suggested_price_sample(market_pair)
        self.calculate_profit_factors()

        for active_order in active_orders:
            # Mark the has_active_buy and has_active_sell flags
            is_buy = active_order.is_buy
            if is_buy:
                has_active_buy = True
            else:
                has_active_sell = True

            # Suppose the active order is hedged on the taker market right now, what's the average price the hedge
            # would happen?
            current_hedging_price = self.c_calculate_effective_hedging_price(
                market_pair, is_buy, active_order.quantity
            )

            # See if it's still profitable to keep the order on maker market. If not, remove it.
            if not self.c_check_if_still_profitable(market_pair, active_order, current_hedging_price):
                continue

            if not self._active_order_canceling:
                continue

            # See if I still have enough balance on my wallet to fill the order on maker market, and to hedge the
            # order on taker market. If not, adjust it.
            if not self.c_check_if_sufficient_balance(market_pair, active_order):
                continue

            # If prices have moved, one side is still profitable, here cancel and
            # place at the next tick.
            if self._current_timestamp > anti_hysteresis_timer:
                if not self.c_check_if_price_has_drifted(market_pair, active_order):
                    need_adjust_order = True
                    continue

        # If order adjustment is needed in the next tick, set the anti-hysteresis timer s.t. the next order adjustment
        # for the same pair wouldn't happen within the time limit.
        if need_adjust_order:
            self._anti_hysteresis_timers[market_pair] = self._current_timestamp + self._anti_hysteresis_duration

        # If effective leverage goes above threshold, then decrease position to manage risk
        # if self.get_effective_leverage(market_pair) > self._perp_leverage * Decimal("1.2"):
        #     self.deleverage_position(market_pair)

        # If there's both an active bid and ask, then there's no need to think about making new limit orders.
        if has_active_buy and has_active_sell:
            return

        # If there are pending taker orders, wait for them to complete
        if self.has_active_taker_order(market_pair):
            return
        
        # If balancing is active and 5 second has passed from last hedge
        if self._entry_status.balancing_active and self._current_timestamp > (self._last_hedge_placed + 30):
            self.logger().info(f"[Level]-balancer-balancing active and past hedge time window")
            self.balance_maker_taker_positions(market_pair)
            return
        
        if self._entry_status.update_ratio:
            if not self.maker_taker_balancing_needed(market_pair):
                prev_ratio = self._entry_status.average_ratio
                if self._is_coin_marginated:
                    if len(self.perp_positions) > 0:
                        pos = self.perp_positions[0]
                        cont_value = market_pair.taker.market.get_value_from_contract(market_pair.taker.trading_pair, pos.amount)
                        self._entry_status.update_entry("taker", cont_value, cont_value / pos.entry_price)
                    else:
                        self._entry_status.update_entry("taker", s_decimal_zero, s_decimal_zero)
                    self.logger().info(f"[Level]-Taker balanced {self._entry_status.taker_entry}")
                self._entry_status.update_entry_ratio()
                self.logger().info(f"[Level]-Entry ratio updated to {self._entry_status.average_ratio:.2f} from {prev_ratio:.2f}")
        
        if self._entry_status.set_bo_ratio:
            if not self.maker_taker_balancing_needed(market_pair):
                self.logger().info(f"[Level]-Base Order balanced and filled")
                self.set_base_order_ratio(market_pair)
                self.set_safety_order_levels(market_pair)
                self.process_maker_sell_fills()
            return

        # See if it's profitable to place a limit order on maker market.
        self.c_check_and_create_new_orders(market_pair, has_active_buy, has_active_sell)

    def log_order_fill_event(self, order_filled_event, exchange):
        # conversion_rate = self.market_conversion_rate(False)
        
        order_id = order_filled_event.order_id
        market = "Maker" if order_id in self._maker_order_ids else "Taker"
        trade_type = "Buy" if order_filled_event.trade_type is TradeType.BUY else "Sell"
        trading_pair = order_filled_event.trading_pair.split("-")
        base_asset = trading_pair[0] if not self._is_coin_marginated else "Cont"
        quote_asset = trading_pair[1]
        base_amount = Decimal(order_filled_event.amount)
        price = Decimal(order_filled_event.price)
        trade_fee = Decimal(order_filled_event.trade_fee.flat_fees[0].amount) if market is "Maker" else s_decimal_zero

        self.logger().info(f"[Filled]-{market}-{trade_type}-{base_amount}-{base_asset}-[Price]-{price:.4f}-[Fee]-{trade_fee}-[ID]-{order_id}-{exchange.name}")

        if trade_fee > s_decimal_zero and exchange.name == "bithumb":
            self.notify_hb_app(f"Coupon-Need to buy fee coupon")

    def order_level_processing(self, order_filled_event):
        is_maker = order_filled_event.order_id in self._maker_order_ids
        is_sell = order_filled_event.trade_type is TradeType.SELL
        entry_status = self._entry_status
        
        order_id = order_filled_event.order_id
        market = "Maker" if order_id in self._maker_order_ids else "Taker"
        trade_type = "Buy" if order_filled_event.trade_type is TradeType.BUY else "Sell"
        self._last_hedge_placed = self._current_timestamp

        if market is "Maker":
            if trade_type is "Sell":
                filled_quote = order_filled_event.amount * order_filled_event.price
                filled_base = order_filled_event.amount

                entry_status.maker_sold(filled_quote, filled_base)
                self.logger().info(f"[Level]-Maker Sold {filled_base} Base @ {entry_status.maker_entry}")
                current_level, current_ratio = self.process_maker_sell_fills()
    
            if trade_type is "Buy":
                filled_base = order_filled_event.amount
                maker_base = entry_status.get_entry_value("maker", "base")
                taker_base = entry_status.get_entry_value("taker", "base")
                
                maker_diff = maker_base - filled_base if (maker_base - filled_base) > s_decimal_zero else s_decimal_zero
                taker_diff = taker_base - filled_base if (taker_base - filled_base) > s_decimal_zero else s_decimal_zero

                maker_remaining = maker_diff / maker_base
                taker_remaining = taker_diff / taker_base
                
                entry_status.update_entry("maker", entry_status.get_entry_value("maker", "quote") * maker_remaining , maker_diff)
                entry_status.update_entry("taker", entry_status.get_entry_value("taker", "quote") * taker_remaining , taker_diff)
            
                self.logger().info(f"[Level]-TP {filled_base} Base-Maker @ {entry_status.maker_entry}-Taker @ {entry_status.taker_entry}")
                entry_status.start_ratio_update()
                entry_status.start_balancing()
                current_level, current_ratio = self.process_maker_sell_fills()

        # if market is "Taker" and entry_status.balancing_active:
        # Taker amount 
        # Coin: quote contract value, base quote / avg price, avg price
        # USDT: quote initial avg * size - current avg * size, base current size - initial size
        if market is "Taker" and entry_status.maker_sold_in_progress:
            filled_quote = order_filled_event.amount * order_filled_event.price
            is_buy = True if trade_type is "Buy" else False

            if self._is_coin_marginated:
                market_pair = list(self._market_pairs.values())[0]
                taker = market_pair.taker
                filled_quote = taker.market.get_value_from_contract(taker.trading_pair, order_filled_event.amount)
            
            filled_base = filled_quote / order_filled_event.price
            entry_status.taker_order_filled(filled_quote, filled_base, is_buy)
            self.logger().info(f"[Level]-Taker {trade_type} {filled_base} Base @ {entry_status.taker_entry}")

            if self._is_coin_marginated and len(self.perp_positions) > 0:
                pos_quote = taker.market.get_value_from_contract(taker.trading_pair, self.perp_positions[0].amount)
                self.logger().info(f"[Level]-Taker Position {pos_quote}-{pos_quote / self.perp_positions[0].entry_price} @ {entry_status.taker_entry}")

    cdef c_did_fill_order(self, object order_filled_event):
        """
        If a limit order previously made to the maker side has been filled, hedge it on the taker side.
        :param order_filled_event: event object
        """
        cdef:
            str order_id = order_filled_event.order_id
            object market_pair = self._market_pair_tracker.c_get_market_pair_from_order_id(order_id)
            object exchange = self._market_pair_tracker.c_get_exchange_from_order_id(order_id)
            tuple order_fill_record
        try:
            self.log_order_fill_event(order_filled_event, exchange)
        except Exception:
            self.log_with_clock(logging.ERROR, "Order fill logging error", exc_info=True)

        # Make sure to only hedge limit orders.
        if market_pair is not None and order_id in self._maker_order_ids:
            limit_order_record = self._sb_order_tracker.c_get_shadow_limit_order(order_id)
            order_fill_record = (limit_order_record, order_filled_event)

            # Store the limit order fill event in a map, s.t. it can be processed in c_check_and_hedge_orders()
            # later.
            if order_filled_event.trade_type is TradeType.BUY:
                if market_pair not in self._order_fill_buy_events:
                    self._order_fill_buy_events[market_pair] = [order_fill_record]
                else:
                    self._order_fill_buy_events[market_pair].append(order_fill_record)

            else:
                if market_pair not in self._order_fill_sell_events:
                    self._order_fill_sell_events[market_pair] = [order_fill_record]
                else:
                    self._order_fill_sell_events[market_pair].append(order_fill_record)

            # Call c_check_and_hedge_orders() to emit the orders on the taker side.
            try:
                self.c_check_and_hedge_orders(market_pair)
            except Exception:
                self.log_with_clock(logging.ERROR, "Unexpected error.", exc_info=True)
        
        self.order_level_processing(order_filled_event)


    cdef c_did_complete_buy_order(self, object order_completed_event):
        """
        Output log message when a bid order (on maker side or taker side) is completely taken.
        :param order_completed_event: event object
        """
        cdef:
            str order_id = order_completed_event.order_id
            object market_pair = self._market_pair_tracker.c_get_market_pair_from_order_id(order_id)
            object exchange = self._market_pair_tracker.c_get_exchange_from_order_id(order_id)
            LimitOrder limit_order_record
            object order_type = order_completed_event.order_type

        if market_pair is not None:
            if order_id in self._maker_order_ids:
                limit_order_record = self._sb_order_tracker.c_get_limit_order(market_pair.maker, order_id)
                self.logger().info(
                    f"Maker buy {order_id} "
                    f"({limit_order_record.quantity} {limit_order_record.base_currency} @ "
                    f"{limit_order_record.price} {limit_order_record.quote_currency}) has been completely filled."
                )
                # self.notify_hb_app_with_timestamp(
                #     f"Maker buy order ({limit_order_record.quantity} {limit_order_record.base_currency} @ "
                #     f"{limit_order_record.price} {limit_order_record.quote_currency}) is filled."
                # )
            else:
                self.logger().info(
                    f"Taker buy order {order_id} for "
                    f"({order_completed_event.base_asset_amount} {order_completed_event.base_asset}) has been completely filled."
                )
                # self.notify_hb_app_with_timestamp(
                #     f"Taker buy order {order_completed_event.base_asset_amount} {order_completed_event.base_asset} is filled."
                # )

    cdef c_did_complete_sell_order(self, object order_completed_event):
        """
        Output log message when a ask order (on maker side or taker side) is completely taken.
        :param order_completed_event: event object
        """
        cdef:
            str order_id = order_completed_event.order_id
            object market_pair = self._market_pair_tracker.c_get_market_pair_from_order_id(order_id)
            object exchange = self._market_pair_tracker.c_get_exchange_from_order_id(order_id)
            LimitOrder limit_order_record

        order_type = order_completed_event.order_type
        if market_pair is not None:
            if order_id in self._maker_order_ids:
                limit_order_record = self._sb_order_tracker.c_get_limit_order(market_pair.maker, order_id)
                self.logger().info(
                    f"Maker sell order {order_id} "
                    f"({limit_order_record.quantity} {limit_order_record.base_currency} @ "
                    f"{limit_order_record.price} {limit_order_record.quote_currency}) has been completely filled."
                )
                # self.notify_hb_app_with_timestamp(
                #     f"Maker sell order ({limit_order_record.quantity} {limit_order_record.base_currency} @ "
                #     f"{limit_order_record.price} {limit_order_record.quote_currency}) is filled."
                # )
            else:
                self.logger().info(
                    f"Taker sell order {order_id} for "
                    f"({order_completed_event.base_asset_amount} {order_completed_event.base_asset}) has been completely filled."
                )
                # self.notify_hb_app_with_timestamp(
                #     f"Taker sell order {order_completed_event.base_asset_amount} {order_completed_event.base_asset} is filled."
                # )

    cdef bint c_check_if_price_has_drifted(self, object market_pair, LimitOrder active_order):
        """
        Given a currently active limit order on maker side, check if its current price is still valid, based on the
        current hedging price on taker market, depth tolerance, and transient orders on the maker market captured by
        recent suggested price samples.

        If the active order's price is no longer valid, the order will be cancelled.

        This function is only used when active order cancellation is enabled.

        :param market_pair: cross exchange market pair
        :param active_order: a current active limit order in the market pair
        :return: True if the order stays, False if the order has been cancelled and we need to re place the orders.
        """
        cdef:
            bint is_buy = active_order.is_buy
            object order_price = active_order.price
            object order_quantity = active_order.quantity
            object suggested_price = self.c_get_market_making_price(market_pair, is_buy, order_quantity)

        if suggested_price != order_price:

            if is_buy:
                if self._logging_options & self.OPTION_LOG_ADJUST_ORDER:
                    self.logger().info(
                        f"[Cancel]-Maker-Buy-Price Drift-The current limit buy order for "
                        f"{active_order.quantity} {market_pair.maker.base_asset} at "
                        f"{order_price:.8g} {market_pair.maker.quote_asset} is now below the suggested order "
                        f"price at {suggested_price}."
                    )
                self.c_cancel_order(market_pair, active_order.client_order_id)
                self.log_with_clock(logging.DEBUG,
                                    f"Current buy order price={order_price}, "
                                    f"suggested order price={suggested_price}")
                return False
            else:
                if self._logging_options & self.OPTION_LOG_ADJUST_ORDER:
                    self.logger().info(
                        f"[Cancel]-Maker-Sell-Price Drift-The current limit sell order for "
                        f"{active_order.quantity} {market_pair.maker.base_asset} at "
                        f"{order_price:.8g} {market_pair.maker.quote_asset} is now below the suggested order "
                        f"price at {suggested_price}."
                    )
                self.c_cancel_order(market_pair, active_order.client_order_id)
                self.log_with_clock(logging.DEBUG,
                                    f"Current sell order price={order_price}, "
                                    f"suggested order price={suggested_price}")
                return False

        return True

    cdef c_get_bid_ask_prices(self, object market_pair):
        cdef:
            str maker_trading_pair = market_pair.maker.trading_pair
            str taker_trading_pair = market_pair.taker.trading_pair
            ExchangeBase maker_market = market_pair.maker.market
            ExchangeBase taker_market = market_pair.taker.market

        maker_bid = maker_market.get_price(maker_trading_pair, False)
        maker_ask = maker_market.get_price(maker_trading_pair, True)
        taker_bid = taker_market.get_price(taker_trading_pair, False)
        taker_ask = taker_market.get_price(taker_trading_pair, True)

        return maker_bid, maker_ask, taker_bid, taker_ask
    
    cdef c_calculate_premium(self, object market_pair, bint is_adjusted):
        maker_bid, maker_ask, taker_bid, taker_ask = self.c_get_bid_ask_prices(market_pair)
        
        taker_bid *= Decimal(str(self._ema.current_value)) if is_adjusted else self._fixed_beta
        taker_ask *= Decimal(str(self._ema.current_value)) if is_adjusted else self._fixed_beta

        premium_tm = taker_bid / maker_bid - 1
        premium_mt = maker_ask / taker_ask - 1
        return premium_tm, premium_mt

    cdef c_check_and_hedge_orders(self, object market_pair):
        """
        Look into the stored and un-hedged limit order fill events, and emit orders to hedge them, depending on
        availability of funds on the taker market.

        :param market_pair: cross exchange market pair
        """
        cdef:
            ExchangeBase taker_market = market_pair.taker.market
            ExchangeBase maker_market = market_pair.maker.market
            str taker_trading_pair = market_pair.taker.trading_pair
            OrderBook taker_order_book = market_pair.taker.order_book
            list buy_fill_records = self._order_fill_buy_events.get(market_pair, [])
            list sell_fill_records = self._order_fill_sell_events.get(market_pair, [])
            object buy_fill_quantity = sum([fill_event.amount for _, fill_event in buy_fill_records])
            object sell_fill_quantity = sum([fill_event.amount for _, fill_event in sell_fill_records])
            object taker_top
            object hedged_order_quantity
            object avg_fill_price

        global s_decimal_zero

        if buy_fill_quantity > 0:
            taker_top = taker_market.c_get_price(taker_trading_pair, False)
            if self._is_coin_marginated:
                # TODO for coin-m convert amount to contracts 
                # amount bought from maker * taker price  = taker value to hedge
                # taker_hedge_value = buy_fill_quantity * taker_top
                taker_hedge_value = buy_fill_quantity * self.perp_positions[0].entry_price if len(self.perp_positions) > 0 else 0
                bought_value_in_contracts = taker_market.get_contract_amount_from_value(taker_trading_pair, taker_hedge_value)
                current_position_contracts = self.perp_positions[0].amount if len(self.perp_positions) > 0 else 0
                hedged_order_quantity = min(bought_value_in_contracts, current_position_contracts)
            else:
                # TODO for usdt-m check available leveraged amount (available balance * leverage)
                # taker_available_balance = taker_market.c_get_available_balance(market_pair.taker.quote_asset) * self._perp_leverage
                # hedged_order_quantity = min(buy_fill_quantity,
                #     taker_available_balance / taker_market.c_get_price(taker_trading_pair, False))
                hedged_order_quantity = buy_fill_quantity

            quantized_hedge_amount = taker_market.quantize_order_amount(taker_trading_pair, Decimal(hedged_order_quantity), taker_top)
            order_price = taker_market.c_get_price_for_volume(taker_trading_pair, False, quantized_hedge_amount).result_price
            order_price *= Decimal("1") - self._slippage_buffer
            order_price = taker_market.quantize_order_price(taker_trading_pair, min(taker_top, order_price))

            order_value = taker_market.get_value_from_contract(taker_trading_pair, quantized_hedge_amount) if self._is_coin_marginated else quantized_hedge_amount * order_price

            if quantized_hedge_amount > s_decimal_zero:
                hedge_id = self.c_place_order(market_pair, False, False, quantized_hedge_amount, order_price)
                del self._order_fill_buy_events[market_pair]
                self._last_hedge_placed = self._current_timestamp
                if self._logging_options & self.OPTION_LOG_MAKER_ORDER_HEDGED:
                    avg_buy_price = (sum([r.price * r.amount for _, r in buy_fill_records]) /
                                    sum([r.amount for _, r in buy_fill_records]))
                    self.logger().info(
                        f"[Hedge]-Maker-Buy-{(avg_buy_price/taker_top):.2f}-{buy_fill_quantity} {market_pair.maker.base_asset}-{avg_buy_price}-{(buy_fill_quantity * avg_buy_price):.0f}-"
                        f"Taker-Sell-{quantized_hedge_amount}-{taker_top}-{order_value}"
                    )

            else:
                self.logger().info(
                    f"Current maker buy fill amount of "
                    f"{buy_fill_quantity} {market_pair.maker.base_asset} is less than the minimum order amount "
                    f"allowed on the taker market. No hedging possible yet."
                )

        if sell_fill_quantity > 0:
            taker_top = taker_market.c_get_price(taker_trading_pair, True)
            if self._is_coin_marginated:
                # TODO for coin-m convert amount to contracts 
                # value sold in taker currency / contract multiplier
                taker_hedge_value = sell_fill_quantity * taker_top
                sold_value_in_contracts = taker_market.get_contract_amount_from_value(taker_trading_pair, taker_hedge_value)
                hedged_order_quantity = sold_value_in_contracts
            else:
                # TODO for usdt-m check available position amount (current short size)
                current_position = self.perp_positions[0].amount if len(self.perp_positions) > 0 else s_decimal_zero
                hedged_order_quantity = min(sell_fill_quantity, -current_position)

            quantized_hedge_amount = taker_market.quantize_order_amount(taker_trading_pair, Decimal(hedged_order_quantity), taker_top)
            order_price = taker_market.get_price_for_volume(taker_trading_pair, True, quantized_hedge_amount).result_price
            order_price *= Decimal("1") + self._slippage_buffer
            order_price = taker_market.quantize_order_price(taker_trading_pair, max(taker_top, order_price))

            order_value = taker_market.get_value_from_contract(taker_trading_pair, quantized_hedge_amount) if self._is_coin_marginated else quantized_hedge_amount * order_price

            if quantized_hedge_amount > s_decimal_zero:
                hedge_id = self.c_place_order(market_pair, True, False, quantized_hedge_amount, order_price)
                del self._order_fill_sell_events[market_pair]
                self._last_hedge_placed = self._current_timestamp
                if self._logging_options & self.OPTION_LOG_MAKER_ORDER_HEDGED:
                    avg_sell_price = (sum([r.price * r.amount for _, r in sell_fill_records]) /
                                    sum([r.amount for _, r in sell_fill_records]))
                    self.logger().info(
                        f"[Hedge]-Maker-Sell-{(avg_sell_price/taker_top):.2f}-{sell_fill_quantity} {market_pair.maker.base_asset}-{avg_sell_price}-{(sell_fill_quantity * avg_sell_price):.0f}-"
                        f"Taker-Buy-{quantized_hedge_amount}-{taker_top}-{order_value}"
                    )
            else:
                self.logger().info(
                    f"Current maker sell fill amount of "
                    f"{sell_fill_quantity} {market_pair.maker.base_asset} is less than the minimum order amount "
                    f"allowed on the taker market. No hedging possible yet."
                )

    def get_minimum_order_size(self, market_pair):
        maker = market_pair.maker
        taker = market_pair.taker

        maker_min_amount = max(Decimal("11000") / maker.get_price(False), maker.market.get_min_order_size(maker.trading_pair))
        taker_min_amount = max(Decimal("11") / taker.get_price(False), taker.market.get_min_order_size(taker.trading_pair))

        return max(maker_min_amount, taker_min_amount)
        

    cdef object c_get_adjusted_limit_order_size(self, object market_pair, bint is_buy):
        """
        Given the proposed order size of a proposed limit order (regardless of bid or ask), adjust and refine the order
        sizing according to either the trade size override setting (if it exists), or the portfolio ratio limit (if
        no trade size override exists).

        Also, this function will convert the input order size proposal from floating point to Decimal by quantizing the
        order size.

        :param market_pair: cross exchange market pair
        :rtype: Decimal
        """
        cdef:
            ExchangeBase maker_market = market_pair.maker.market
            str trading_pair = market_pair.maker.trading_pair
        if self._order_amount and self._order_amount > 0:
            base_order_size = self._order_amount
            current_level = self._entry_status.current_entry_level
            current_base_fill = self._entry_status.get_entry_value("maker", "base")
            levels = self._order_levels
            if is_buy:
                # full buy when level is zero or completely filled
                if current_level == 0 or (current_level == 1 and levels[current_level]["filled"] < Decimal("0.05")):
                    remaining_amount = current_base_fill
                # if level 1 and less than 5% buy max
                # if level greater than 2 and less than 5% buy up to prev level
                elif (current_level > 1 and current_level < len(levels) and levels[current_level]["filled"] < Decimal("0.05")) or current_level >= len(levels):
                    prev_level_amount = levels[current_level - 2]["cum_amount"]
                    remaining_amount = (current_base_fill - prev_level_amount)
                else:
                    prev_level_amount = levels[current_level - 1]["cum_amount"]
                    remaining_amount = (current_base_fill - prev_level_amount)
            else:
                # full sell when in last level
                if current_level >= len(levels):
                    remaining_amount = base_order_size
                else:
                    # remaining_amount = (current_level_amount - self._entry_status.get_entry_value("maker", "base")) * Decimal("1.02")
                    remaining_amount = levels[current_level]["cum_amount"] * Decimal("1.05") - current_base_fill
            
            base_order_size = max(min(base_order_size, remaining_amount), self.get_minimum_order_size(market_pair))
            return maker_market.quantize_order_amount(trading_pair, Decimal(base_order_size))
        else:
            return self.c_get_order_size_after_portfolio_ratio_limit(market_pair)

    cdef object c_get_order_size_after_portfolio_ratio_limit(self, object market_pair):
        """
        Given the proposed order size of a proposed limit order (regardless of bid or ask), adjust the order sizing
        according to the portfolio ratio limit.

        Also, this function will convert the input order size proposal from floating point to Decimal by quantizing the
        order size.

        :param market_pair: cross exchange market pair
        :rtype: Decimal
        """
        cdef:
            ExchangeBase maker_market = market_pair.maker.market
            str trading_pair = market_pair.maker.trading_pair
            object base_balance = maker_market.c_get_balance(market_pair.maker.base_asset)
            object quote_balance = maker_market.c_get_balance(market_pair.maker.quote_asset)
            object current_price = (maker_market.c_get_price(trading_pair, True) +
                                    maker_market.c_get_price(trading_pair, False)) * Decimal(0.5)
            object maker_portfolio_value = base_balance + quote_balance / current_price
            object adjusted_order_size = maker_portfolio_value * self._order_size_portfolio_ratio_limit

        return maker_market.quantize_order_amount(trading_pair, Decimal(adjusted_order_size))

    cdef object c_get_market_making_size(self,
                                         object market_pair,
                                         bint is_buy):
        """
        Get the ideal market making order size given a market pair and a side.

        This function does a few things:
         1. Calculate the largest order size possible given the current balances on both maker and taker markets.
         2. Calculate the largest order size possible that's still profitable after hedging.

        For coin marginated, it should round to nearest contract amount
        quantity * maker price / conversion_rate

        :param market_pair: The cross exchange market pair to calculate order price/size limits.
        :param is_buy: Whether the order to make will be bid or ask.
        :return: a Decimal which is the size of maker order.
        """
        cdef:
            str maker_trading_pair = market_pair.maker.trading_pair
            str taker_trading_pair = market_pair.taker.trading_pair
            ExchangeBase maker_market = market_pair.maker.market
            ExchangeBase taker_market = market_pair.taker.market

        user_order = self.c_get_adjusted_limit_order_size(market_pair, is_buy)
        taker_is_buy = False if is_buy else True
        taker_price = taker_market.c_get_price(taker_trading_pair, taker_is_buy)
        
        if self._is_coin_marginated:
            contract_amount = taker_market.get_contract_amount_from_value(taker_trading_pair, user_order * taker_price)
            user_order = taker_market.get_value_from_contract(taker_trading_pair, contract_amount) / taker_price

        if is_buy:
            # allowed until spot quote balance becomes 0
            maker_balance_in_quote = maker_market.c_get_available_balance(market_pair.maker.quote_asset)
            # _, maker_balance_in_quote = self.get_adjusted_available_balance(market_pair)
            active_buys_in_quote = sum([b.price * b.quantity for _, b in self.active_buys])
            maker_balance_in_quote += active_buys_in_quote

            try:
                maker_price = maker_market.c_get_price_for_quote_volume(
                    maker_trading_pair, True, maker_balance_in_quote
                ).result_price
                
            except ZeroDivisionError:
                assert user_order == s_decimal_zero
                return s_decimal_zero

            maker_price = maker_market.c_get_price(maker_trading_pair, is_buy) if Decimal.is_nan(maker_price) else maker_price
            maker_balance = maker_balance_in_quote / maker_price * Decimal("0.995")

            # maker buy > taker sell
            # coin-m: decrease long position (max allowed current position amount)
            # usdt-m: increase short position (max allowed available quote * lev)
            if not self._is_coin_marginated:
                taker_balance_in_quote = taker_market.c_get_available_balance(market_pair.taker.quote_asset) * self._perp_leverage
                try:
                    taker_price = taker_market.c_get_price_for_quote_volume(
                        taker_trading_pair, True, taker_balance_in_quote
                    ).result_price
                    
                except ZeroDivisionError:
                    assert user_order == s_decimal_zero
                    return s_decimal_zero
                
                taker_balance = taker_balance_in_quote / taker_price * Decimal("0.995")

            order_amount = min(maker_balance, user_order) if self._is_coin_marginated else min(maker_balance, user_order, taker_balance)

        else:
            # allowed until sell all base asset
            maker_balance = maker_market.c_get_available_balance(market_pair.maker.base_asset) * Decimal("0.995")

            # maker sell > taker buy
            # coin-m: increase long position (max allowed available base * lev)
            # usdt-m: decrease short position (max allowed current position amount)
            if self._is_coin_marginated:
                taker_balance = taker_market.c_get_available_balance(market_pair.taker.base_asset) * self._perp_leverage * Decimal("0.995")
                min_taker_balance = taker_market.get_value_from_contract(taker_trading_pair, 1) / taker_price

                if taker_balance < min_taker_balance:
                    return s_decimal_zero

            order_amount = min(maker_balance, user_order, taker_balance) if self._is_coin_marginated else min(maker_balance, user_order)
        
        # Just in case order amount with fee exceeds available balance
        # order_amount *= Decimal("0.995")

        if self._is_coin_marginated:
            order_amount = taker_market.get_contract_amount_from_value(taker_trading_pair, order_amount * taker_price)
            order_amount = order_amount if order_amount > s_decimal_zero else s_decimal_zero

        order_amount = taker_market.quantize_order_amount(taker_trading_pair, order_amount, taker_price)

        if self._is_coin_marginated:
            order_amount = taker_market.get_value_from_contract(taker_trading_pair, order_amount) / taker_price
            order_amount = maker_balance if maker_balance < order_amount else order_amount

        return maker_market.quantize_order_amount(maker_trading_pair, order_amount)

    def get_adjusted_available_balance(self, market_pair):
        """
        Calculates the available balance, plus the amount attributed to orders.
        :return: (base amount, quote amount) in Decimal
        """

        maker = market_pair.maker
        base_balance = maker.market.get_available_balance(maker.base_asset)
        quote_balance = maker.market.get_available_balance(maker.quote_asset)
        orders = []

        if market_pair.maker in self._sb_order_tracker.get_limit_orders():
            orders = list(self._sb_order_tracker.get_limit_orders()[market_pair.maker].values())

        for order in orders:
            if order.is_buy:
                quote_balance += order.quantity * order.price
            else:
                base_balance += order.quantity

        return base_balance, quote_balance

    # def deleverage_position(self, market_pair):
    #     """
    #     When close to liquidation price
    #     Called when effective levereage is greater than the threshold
    #     - calculate amount needed to go below threshold
    #     - send maker order, when filled automatically hedge
    #     - 
    #     """
    #     taker_market = market_pair.taker.market
    #     maker_market = market_pair.maker.market
    #     taker_trading_pair = market_pair.taker_trading_pair
    #     maker_trading_pair = market_pair.maker_trading_pair

    #     taker_asset_in_base, _ = self.get_total_assets(market_pair)
    #     target_base_amount = taker_asset_in_base * self._perp_leverage
    #     perp_amount = abs(self.perp_positions[0].amount)
    #     maker_order_size = perp_amount - target_base_amount

    #     if self._is_coin_marginated:
    #         target_contract_amount = taker_market.get_contract_amount_from_value(taker_trading_pair, target_base_amount * taker_mid)
    #         taker_order_size = perp_amount - target_contract_amount
    #         maker_order_size = taker_market.get_value_from_contract(taker_trading_pair, taker_order_size) / taker_mid

    #     quantized_maker_order = maker_market.quantize_order_amount(maker_trading_pair, maker_order_size)

    #     # to minimize position exposure
    #     # coin-m: sell taker buy maker
    #     # usdt-m: buy taker sell maker
    #     maker_is_buy = True if self._is_coin_marginated else False
    #     maker_price_buffer = Decimal("1.01") if self._is_coin_marginated else Decimal("0.99")
    #     maker_price = maker_market.c_get_price_for_volume(maker_trading_pair, maker_is_buy, quantized_maker_order).result_price * maker_price_buffer
    #     quantized_maker_price = maker_market.quantize_order_price(maker_trading_pair, maker_price)
        
    #     if maker_order_size > s_decimal_zero:
    #         eff_leverage = self.get_effective_leverage(market_pair)
    #         self.logger().info(
    #             f"[Risk]-Maker-{"Buy" if maker_is_buy else "Sell"}-{quantized_maker_order} {market_pair.maker.base_asset}-{quantized_maker_price} {market_pair.maker.quote_asset}-"
    #             f"Eff Lev-{eff_leverage:.2f}x"
    #         )
    #         self.c_place_order(market_pair, maker_is_buy, True, quantized_maker_order, quantized_maker_price)


    def calibrate_hedging(self, market_pair):
        """
        Need to calibrate hedging to match whenever minimum hedging difference occur
        If no active hedging order, then execute
        Check differences and whenever goes above minimum order size execute

        COIN-M: 
        - match spot market value with contract value (exchange rate change, regular hedging differences)
        - market down / prem up: less contracts, more krw
        USDT-M: match spot market base amount with futures position (regular hedging differences)
        """
        
        taker_market = market_pair.taker.market
        taker_trading_pair = market_pair.taker.trading_pair

        if self._is_coin_marginated:
            buy_size = self.c_get_market_making_size(market_pair, True)
            if buy_size == s_decimal_zero:
                cont_amount = self.perp_positions[0].amount if len(self.perp_positions) > 0 else s_decimal_zero
            
            if cont_amount > s_decimal_zero:
                quantized_hedge_amount = taker_market.quantize_order_amount(taker_trading_pair, Decimal(cont_amount))
                order_price = taker_market.c_get_price_for_volume(taker_trading_pair, False, quantized_hedge_amount).result_price
                order_price *= Decimal("1") - self._slippage_buffer
                order_price = taker_market.quantize_order_price(taker_trading_pair, order_price)

                hedge_id = self.c_place_order(market_pair, False, False, quantized_hedge_amount, order_price)
                self.logger().info(f"[Calibrate]-Taker-Sell-{quantized_hedge_amount}-{order_price}")
        
        if not self._is_coin_marginated:
            sell_size = self.c_get_market_making_size(market_pair, False)

            if sell_size == s_decimal_zero:
                cont_amount = abs(self.perp_positions[0].amount) if len(self.perp_positions) > 0 else s_decimal_zero
            
            if cont_amount > s_decimal_zero:
                quantized_hedge_amount = taker_market.quantize_order_amount(taker_trading_pair, Decimal(cont_amount))
                order_price = taker_market.get_price_for_volume(taker_trading_pair, True, quantized_hedge_amount).result_price
                order_price *= Decimal("1") + self._slippage_buffer
                order_price = taker_market.quantize_order_price(taker_trading_pair, order_price)

                hedge_id = self.c_place_order(market_pair, True, False, quantized_hedge_amount, order_price)
                self.logger().info(f"[Calibrate]-Taker-Buy-{quantized_hedge_amount}-{order_price}")
    

    def calculate_profit_factors(self):
        market_pair = list(self._market_pairs.values())[0]
        ema = Decimal(str(self._ema.current_value))
        maker_last = market_pair.maker.get_price_by_type(PriceType.LastTrade)
        taker_last = market_pair.taker.get_price_by_type(PriceType.LastTrade)

        # apply std
        std = Decimal(str(self._std.std_dev(self._std_length))) / ema
        std = s_decimal_zero if Decimal.is_nan(std) else std

        # apply disparity
        disparity = (maker_last / taker_last) / ema - s_decimal_one
        disp_sell = s_decimal_zero
        disp_buy = s_decimal_zero
        
        if not Decimal.is_nan(disparity):
            disp_sell = abs(disparity) if disparity >= self._disparity_sensitivity else s_decimal_zero
            disp_buy = abs(disparity) if disparity <= -self._disparity_sensitivity else s_decimal_zero

        self._disparity = disp_sell, disp_buy

        # # apply trend factor
        # trend = Decimal(str(self._trend_sma.trend_slope))
        # trend_sell = abs(trend) if trend > 0 else s_decimal_zero
        # trend_buy = abs(trend) if trend < 0 else s_decimal_zero
        # self._trend = trend_sell, trend_buy
        self._trend = s_decimal_zero, s_decimal_zero

        # position factor increase spread when maker sells (quote ratio increases) quadratic or exponential
        quote_ratio = Decimal(str(self.get_maker_entry_ratio(market_pair)))
        normalized_ratio = min(quote_ratio, self._sell_quote_threshold) / self._sell_quote_threshold
        self._sell_adj_spread = (normalized_ratio**2) * self._sell_adj_factor / Decimal("100")

        self._sell_profit = self._min_profitability + self._sell_adj_spread
        self._sell_profit += (std * self._std_factor)
        # self._sell_profit += (trend_sell * self._trend_factor)
        self._sell_profit += (disp_sell * self._disparity_factor)
        self._sell_profit *= self._sell_profit_factor
        
        self._buy_profit = self._min_profitability
        self._buy_profit += (std * self._std_factor)
        # self._buy_profit += (trend_buy * self._trend_factor)
        self._buy_profit += (disp_buy * self._disparity_factor)
        # self._buy_profit *= self._buy_profit_factor

        self._sell_profit = self._min_profitability if Decimal.is_nan(self._sell_profit) else self._sell_profit
        self._buy_profit = self._min_profitability if Decimal.is_nan(self._buy_profit) else self._buy_profit        


    def get_profitability(self, market_pair, is_buy):
        # centered around 0 (conversion adjusted price)
        if is_buy:
            return max(self._buy_profit, self._min_profitability) if self._buy_profit != s_decimal_nan else self._min_profitability
        else:
            return max(self._sell_profit, self._min_profitability) if self._sell_profit != s_decimal_nan else self._min_profitability
        
    def get_min_profit_ratio(self):
        entry = self._entry_status
        if self._is_grid:
            current_level = entry.current_entry_level
            if current_level == 0:
                return entry.average_ratio / (1 + self._min_profitability * self._buy_profit_factor)
            elif current_level == 1:
                if self._order_levels[current_level]["filled"] < Decimal("0.2"):
                    return entry.average_ratio / (1 + self._min_profitability * self._buy_profit_factor)
            elif current_level >= len(self._order_levels):
                return self._order_levels[current_level - 2]["ratio"]
            else:
                if self._order_levels[current_level]["filled"] >= Decimal("0.2"):
                    return self._order_levels[current_level - 1]["ratio"]
                else:
                    return self._order_levels[current_level - 2]["ratio"]
        else:
            return entry.average_ratio / (1 + self._min_profitability * self._buy_profit_factor)

    cdef object c_get_market_making_price(self,
                                          object market_pair,
                                          bint is_buy,
                                          object size):
        """
        Get the ideal market making order price given a market pair, side and size.

        The price returned is calculated by adding the profitability to vwap of hedging it on the taker side.
        or if it's not possible to hedge the trade profitably, then the returned order price will be none.

        :param market_pair: The cross exchange market pair to calculate order price/size limits.
        :param is_buy: Whether the order to make will be bid or ask.
        :param size: size of the order.
        :return: a Decimal which is the price or None if order cannot be hedged on the taker market
        """
        cdef:
            str taker_trading_pair = market_pair.taker.trading_pair
            ExchangeBase maker_market = market_pair.maker.market
            ExchangeBase taker_market = market_pair.taker.market
            OrderBook taker_order_book = market_pair.taker.order_book
            OrderBook maker_order_book = market_pair.maker.order_book
            object sample_top_bid = s_decimal_nan
            object sample_top_ask = s_decimal_nan
            object top_bid_price = s_decimal_nan
            object top_ask_price = s_decimal_nan
            object price_below_ask = s_decimal_nan

        sample_top_bid, sample_top_ask = self.c_get_top_bid_ask_from_price_samples(market_pair)
        top_bid_price, top_ask_price = self.c_get_top_bid_ask(market_pair)

        if self._asset_price_delegate is not None:
            ref_bid, ref_ask = self.get_ref_bid_ask_price()

        if is_buy:
            if not Decimal.is_nan(sample_top_bid):
                # Calculate the next price above top bid
                price_quantum = maker_market.c_get_order_price_quantum(
                    market_pair.maker.trading_pair,
                    sample_top_bid
                )
                price_above_bid_sample = (ceil(sample_top_bid / price_quantum) + 1) * price_quantum
                price_above_bid = (ceil(top_bid_price / price_quantum) + 1) * price_quantum

            # you are buying on the maker market and selling on the taker market
            taker_hedge_sell_price = self.c_calculate_effective_hedging_price(market_pair, is_buy, size)

            if Decimal.is_nan(taker_hedge_sell_price):
                return s_decimal_nan

            maker_buy_price = taker_hedge_sell_price / (1 + self.get_profitability(market_pair, is_buy))
            maker_buy_price = (floor(maker_buy_price / price_quantum)) * price_quantum
            ref_bid = price_above_bid if self._asset_price_delegate is None else ref_bid

            min_tp_price = maker_buy_price
            taker_price = (taker_hedge_sell_price / self.market_conversion_rate(True))
            if self._entry_status.average_ratio != s_decimal_zero and self._use_min_profit:
                min_tp_price = taker_price * self.get_min_profit_ratio()
                min_tp_price = (floor(min_tp_price / price_quantum)) * price_quantum

            if self._adjust_orders_enabled:
                # If maker bid order book is not empty
                if not Decimal.is_nan(price_above_bid_sample):
                    maker_buy_price = min(maker_buy_price, min_tp_price, price_above_bid_sample, price_above_bid, ref_bid)
            
            if self._is_grid:
                if not Decimal.is_nan(price_above_bid_sample):
                    if self._entry_status.average_ratio == s_decimal_zero:
                        maker_buy_price = min(maker_buy_price, min_tp_price, price_above_bid_sample, price_above_bid, ref_bid)
                    else:
                        maker_buy_price = min(min_tp_price, price_above_bid_sample, price_above_bid, ref_bid)
                    
            price_quantum = maker_market.c_get_order_price_quantum(
                market_pair.maker.trading_pair,
                maker_buy_price
            )

            # Rounds down for ensuring profitability
            maker_buy_price = (floor(maker_buy_price / price_quantum)) * price_quantum
            return maker_buy_price
        else:
            if not Decimal.is_nan(sample_top_ask):
                # Calculate the next price below top ask and above top bid
                price_quantum = maker_market.c_get_order_price_quantum(
                    market_pair.maker.trading_pair,
                    sample_top_ask
                )
                price_below_ask_sample = (floor(sample_top_ask / price_quantum) - 1) * price_quantum
                price_below_ask = (floor(top_ask_price / price_quantum) - 1) * price_quantum

            # You are selling on the maker market and buying on the taker market
            taker_hedge_buy_price = self.c_calculate_effective_hedging_price(market_pair, is_buy, size)

            if Decimal.is_nan(taker_hedge_buy_price):
                return s_decimal_nan
                
            maker_sell_price = taker_hedge_buy_price * (1 + self.get_profitability(market_pair, is_buy))
            maker_sell_price = (ceil(maker_sell_price / price_quantum)) * price_quantum
            ref_ask = price_below_ask if self._asset_price_delegate is None else ref_ask

            max_entry_price = maker_sell_price
            if self._entry_status.current_entry_ratio != s_decimal_zero and self._use_min_profit:
                taker_price = (taker_hedge_buy_price / self.market_conversion_rate(True))
                max_entry_price = taker_price * self._entry_status.current_entry_ratio
                max_entry_price = (ceil(max_entry_price / price_quantum)) * price_quantum

            # If your ask is lower than the the top ask, increase it to just one tick below top ask. Sampled top ask can be below maker price (immediate take)
            if self._adjust_orders_enabled:
                # If maker ask order book is not empty
                if not Decimal.is_nan(price_below_ask_sample):
                    maker_sell_price = max(maker_sell_price, max_entry_price, price_below_ask_sample, price_below_ask, ref_ask)

            price_quantum = maker_market.c_get_order_price_quantum(
                market_pair.maker.trading_pair,
                maker_sell_price
            )

            # Rounds up for ensuring profitability
            maker_sell_price = (ceil(maker_sell_price / price_quantum)) * price_quantum
            return maker_sell_price

    cdef object c_calculate_effective_hedging_price(self,
                                                    object market_pair,
                                                    bint is_buy,
                                                    object size):
        """
        :param market_pair: The cross exchange market pair to calculate order price/size limits.
        :param is_buy: Whether the order to make will be bid or ask.
        :param size: The size of the maker order.
        :return: a Decimal which is the hedging price (Taker Price in Maker converted unit)
        """
        cdef:
            str taker_trading_pair = market_pair.taker.trading_pair
            ExchangeBase taker_market = market_pair.taker.market
        
        # Calculate the next price from the top, and the order size limit.
        taker_is_buy = False if is_buy else True

        if self._is_coin_marginated:
            taker_price = taker_market.c_get_price(taker_trading_pair, taker_is_buy)
            size = taker_market.get_contract_amount_from_value(taker_trading_pair, size * taker_price)

        try:
            taker_price = taker_market.c_get_vwap_for_volume(taker_trading_pair, taker_is_buy, Decimal(size)).result_price
        except ZeroDivisionError:
            return s_decimal_nan

        # If quote assets are not same, convert them from taker's quote asset to maker's quote asset
        if market_pair.maker.quote_asset != market_pair.taker.quote_asset:
            taker_price *= self.market_conversion_rate(True)

        return taker_price
        

    cdef tuple c_get_suggested_price_samples(self, object market_pair):
        """
        Get the queues of order book price samples for a market pair.

        :param market_pair: The market pair under which samples were collected for.
        :return: (bid order price samples, ask order price samples)
        """
        if market_pair in self._suggested_price_samples:
            return self._suggested_price_samples[market_pair]
        return deque(), deque()

    cdef tuple c_get_top_bid_ask(self, object market_pair):
        """
        Calculate the top bid and ask using top depth tolerance in maker order book

        :param market_pair: cross exchange market pair
        :return: (top bid: Decimal, top ask: Decimal)
        """
        cdef:
            str trading_pair = market_pair.maker.trading_pair
            ExchangeBase maker_market = market_pair.maker.market

        if self._top_depth_tolerance == 0:
            top_bid_price = maker_market.c_get_price(trading_pair, False)

            top_ask_price = maker_market.c_get_price(trading_pair, True)

        else:
            # Use bid entries in maker order book
            top_bid_price = maker_market.c_get_price_for_volume(trading_pair,
                                                                False,
                                                                self._top_depth_tolerance).result_price

            # Use ask entries in maker order book
            top_ask_price = maker_market.c_get_price_for_volume(trading_pair,
                                                                True,
                                                                self._top_depth_tolerance).result_price

        return top_bid_price, top_ask_price

    cdef c_take_suggested_price_sample(self, object market_pair):
        """
        Record the bid and ask sample queues.

        These samples are later taken to check if price has drifted for new limit orders, s.t. new limit orders can
        properly take into account transient orders that appear and disappear frequently on the maker market.

        :param market_pair: cross exchange market pair
        """
        if ((self._last_timestamp // self.ORDER_ADJUST_SAMPLE_INTERVAL) <
                (self._current_timestamp // self.ORDER_ADJUST_SAMPLE_INTERVAL)):
            if market_pair not in self._suggested_price_samples:
                self._suggested_price_samples[market_pair] = (deque(), deque())

            top_bid_price, top_ask_price = self.c_get_top_bid_ask_from_price_samples(market_pair)

            bid_price_samples_deque, ask_price_samples_deque = self._suggested_price_samples[market_pair]
            bid_price_samples_deque.append(top_bid_price)
            ask_price_samples_deque.append(top_ask_price)
            while len(bid_price_samples_deque) > self.ORDER_ADJUST_SAMPLE_WINDOW:
                bid_price_samples_deque.popleft()
            while len(ask_price_samples_deque) > self.ORDER_ADJUST_SAMPLE_WINDOW:
                ask_price_samples_deque.popleft()

    cdef tuple c_get_top_bid_ask_from_price_samples(self,
                                                    object market_pair):
        """
        Calculate the top bid and ask using earlier samples

        :param market_pair: cross exchange market pair
        :return: (top bid, top ask)
        """
        # Incorporate the past bid & ask price samples.
        current_top_bid_price, current_top_ask_price = self.c_get_top_bid_ask(market_pair)

        bid_price_samples, ask_price_samples = self.c_get_suggested_price_samples(market_pair)

        if not any(Decimal.is_nan(p) for p in bid_price_samples) and not Decimal.is_nan(current_top_bid_price):
            top_bid_price = max(list(bid_price_samples) + [current_top_bid_price])
        else:
            top_bid_price = current_top_ask_price

        if not any(Decimal.is_nan(p) for p in ask_price_samples) and not Decimal.is_nan(current_top_ask_price):
            top_ask_price = min(list(ask_price_samples) + [current_top_ask_price])
        else:
            top_ask_price = current_top_ask_price

        return top_bid_price, top_ask_price

    cdef bint c_check_if_still_profitable(self,
                                          object market_pair,
                                          LimitOrder active_order,
                                          object current_hedging_price):
        """
        Check whether a currently active limit order should be cancelled or not, according to profitability metric.

        If active order canceling is enabled (e.g. for centralized exchanges), then the min profitability config is
        used as the threshold. If it is disabled (e.g. for decentralized exchanges), then the cancel order threshold
        is used instead.

        :param market_pair: cross exchange market pair
        :param active_order: the currently active order to check for cancellation
        :param current_hedging_price: the current average hedging price on taker market for the limit order
        :return: True if the limit order stays, False if the limit order is being cancelled.
        """
        cdef:
            bint is_buy = active_order.is_buy
            str limit_order_type_str = "Buy" if is_buy else "Sell"
            object order_price = active_order.price
            object cancel_order_threshold

        cancel_order_threshold = self.get_profitability(market_pair, is_buy) if self._active_order_canceling else self._cancel_order_threshold

        if current_hedging_price is None:
            if self._logging_options & self.OPTION_LOG_REMOVING_ORDER:
                self.logger().info(
                    f"[Cancel]-Maker-{limit_order_type_str}-"
                    f"{order_price:.8g} {market_pair.maker.quote_asset} is no longer profitable. "
                )
            self.c_cancel_order(market_pair, active_order.client_order_id)
            return False

        ema = Decimal(self._ema.current_value)

        if not is_buy and self._entry_status.current_entry_ratio > s_decimal_zero:
            entry_ratio = max(self._entry_status.current_entry_ratio, ema * (1 + self.get_profitability(market_pair, is_buy)))
            entry_ratio = self._entry_status.current_entry_ratio if self._is_grid else entry_ratio
            cancel_order_threshold = entry_ratio / ema - 1
        
        if is_buy and self._entry_status.average_ratio > s_decimal_zero:
            tp_band_ratio = ema / (1 + self._buy_profit)
            tp_ratio = min(self.get_min_profit_ratio(), tp_band_ratio) if self._use_min_profit else tp_band_ratio
            tp_ratio = self.get_min_profit_ratio() if self._is_grid else tp_ratio
            cancel_order_threshold = 1 - tp_ratio / ema

        # When current hedge price is lower than old hedge price 
        # When current sell price is higher than old sell price
        if ((is_buy and current_hedging_price < order_price * (1 + cancel_order_threshold)) or
                (not is_buy and order_price < current_hedging_price * (1 + cancel_order_threshold))):

            if self._logging_options & self.OPTION_LOG_REMOVING_ORDER:
                # log current profitability
                self.logger().info(
                    f"[Cancel]-Maker-{limit_order_type_str}-Profitability-"
                    f"{order_price:.8g} {market_pair.maker.quote_asset} is no longer profitable. "
                )
            self.c_cancel_order(market_pair, active_order.client_order_id)
            return False
        
        # bid_threshold, ask_threshold, _, _ = self.c_get_bid_ask_prices(market_pair)
        # adj_tm, adj_mt = self.c_calculate_premium(market_pair, True)

        # if self._asset_price_delegate is not None:
        #     ref_bid, ref_ask = self.get_ref_bid_ask_price()
        #     bid_threshold = min(bid_threshold, ref_bid) if self._delegate_for == "maker" else bid_threshold
        #     ask_threshold = max(ask_threshold, ref_ask) if self._delegate_for == "maker" else ask_threshold

        # # if ref_ask is higher than maker_ask
        # threshold = Decimal("0.0003")
        # if (is_buy and adj_tm > (self._buy_profit + threshold)) or (not is_buy and adj_mt > (self._sell_profit + threshold)):
        #     if (is_buy and order_price < bid_threshold) or (not is_buy and order_price > ask_threshold):
        #         if self._logging_options & self.OPTION_LOG_REMOVING_ORDER:
        #             # log current profitability
        #             self.logger().info(
        #                 f"[Cancel]-Maker-{limit_order_type_str}-Spread-"
        #                 f"{order_price:.8g} {market_pair.maker.quote_asset} is too far from proper profitable. "
        #             )
        #         self.c_cancel_order(market_pair, active_order.client_order_id)
        #         return False
        return True

    cdef bint c_check_if_sufficient_balance(self, object market_pair, LimitOrder active_order):
        """
        Check whether there's enough asset balance for a currently active limit order. If there's not enough asset
        balance for the order (e.g. because the required asset has been moved), cancel the active order.

        This function is only used when active order cancelled is enabled.

        :param market_pair: cross exchange market pair
        :param active_order: current limit order
        :return: True if there's sufficient balance for the limit order, False if there isn't and the order is being
                 cancelled.
        """
        cdef:
            bint is_buy = active_order.is_buy
            object order_price = active_order.price
            object user_order = self.c_get_adjusted_limit_order_size(market_pair, is_buy)
            ExchangeBase maker_market = market_pair.maker.market
            ExchangeBase taker_market = market_pair.taker.market
            object order_size_limit
            str taker_trading_pair = market_pair.taker.trading_pair

        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_taker_maker_conversion_rate()

        # for taker check availabla balance * leverage
        if is_buy:
            maker_quote = maker_market.c_get_balance(market_pair.maker.quote_asset)
            order_size_limit = maker_quote / order_price
            # taker_base = taker_market.c_get_balance(market_pair.taker.base_asset) * base_rate
            # order_size_limit = min(taker_base, maker_quote / order_price)

        else:
            maker_base = maker_market.c_get_balance(market_pair.maker.base_asset)
            order_size_limit = maker_base
            # taker_quote = taker_market.c_get_balance(market_pair.taker.quote_asset)
            # taker_slippage_adjustment_factor = Decimal("1") + self._slippage_buffer
            # taker_price = taker_market.c_get_price_for_quote_volume(
            #     taker_trading_pair, True, taker_quote
            # ).result_price
            # adjusted_taker_price = taker_price * taker_slippage_adjustment_factor
            # order_size_limit = min(maker_base, taker_quote / adjusted_taker_price)

        quantized_size_limit = maker_market.quantize_order_amount(active_order.trading_pair, order_size_limit)

        if active_order.quantity > quantized_size_limit:
            if self._logging_options & self.OPTION_LOG_ADJUST_ORDER:
                self.logger().info(
                    f"Order size limit ({order_size_limit:.8g}) "
                    f"is now less than the current active order amount ({active_order.quantity:.8g}). "
                    f"Going to adjust the order."
                )
            self.c_cancel_order(market_pair, active_order.client_order_id)
            return False
        return True

    def market_conversion_rate(self, is_adjusted) -> Decimal:
        """
        Return price conversion rate for a taker market (to convert it into maker base asset value)
        """
        _, _, quote_rate, _, _, base_rate = self.get_taker_maker_conversion_rate()
        if is_adjusted and self._enable_reg_offset:
            return Decimal(str(self._ema.current_value))
        else:
            return quote_rate / base_rate

    def is_reset_condition(self, market_pair):
        maker = market_pair.maker
        taker = market_pair.taker

        maker_price = maker.market.get_price(maker.trading_pair, True)
        maker_balance = maker.quote_balance
        maker_amount = maker.quote_balance / maker_price * Decimal("0.995")

        maker_reset = maker.market.quantize_order_amount(maker.trading_pair, maker_amount, maker_price)

        taker_price = taker.market.get_price(taker.trading_pair, True)
        if self._is_coin_marginated:
            maker_amount = taker.market.get_contract_amount_from_value(taker.trading_pair, maker_amount * taker_price)

        taker_reset = taker.market.quantize_order_amount(taker.trading_pair, maker_amount, taker_price)

        return maker_reset == s_decimal_zero or taker_reset == s_decimal_zero
    
    def is_within_range(self, market_pair, is_buy, price):
        if self._use_within_range:
            maker = market_pair.maker
            maker_price = maker.market.get_price(maker.trading_pair, not is_buy)

            if is_buy:
                return price > maker_price * (s_decimal_one - self._min_profitability)
            else:
                return price < maker_price * (s_decimal_one + self._min_profitability)
        else:
            return True

    cdef c_check_and_create_new_orders(self, object market_pair, bint has_active_buy, bint has_active_sell):
        """
        Check and account for all applicable conditions for creating new limit orders (e.g. profitability, what's the
        right price given depth tolerance and transient orders on the market, account balances, etc.), and create new
        limit orders for market making.

        :param market_pair: cross exchange market pair
        :param has_active_buy: True if there's already an active bid on the maker side, False otherwise
        :param has_active_sell: True if there's already an active ask on the maker side, False otherwise
        """
        cdef:
            object effective_hedging_price

        # if there is no active bid, place bid again
        if not has_active_buy:
            buy_size = self.c_get_market_making_size(market_pair, True)
            
            # check if bid size is greater than maker and taker market minimum order value
            if buy_size > s_decimal_zero:
                buy_price = self.c_get_market_making_price(market_pair, True, buy_size)

                if not Decimal.is_nan(buy_price) and self.is_within_range(market_pair, True, buy_price):
                    effective_hedging_price = self.c_calculate_effective_hedging_price(
                        market_pair,
                        True,
                        buy_size
                    )
                    effective_hedging_price_adjusted = effective_hedging_price / self.market_conversion_rate(True)
                    if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                        self.logger().debug(
                            f"[Create]-Maker-Buy-{buy_size} {market_pair.maker.base_asset}-{buy_price} {market_pair.maker.quote_asset}-"
                            f"hedging price-{effective_hedging_price:.8f} {market_pair.maker.quote_asset}-"
                            f"rate adjusted-{effective_hedging_price_adjusted:.8f} {market_pair.taker.quote_asset}"
                        )
                    order_id = self.c_place_order(market_pair, True, True, buy_size, buy_price)
                else:
                    if self._logging_options & self.OPTION_LOG_NULL_ORDER_SIZE:
                        self.logger().info(
                            f"({market_pair.maker.trading_pair})"
                            f"Order book on taker is too thin to place order for size: {buy_size}"
                            f"Reduce order_size_portfolio_ratio_limit"
                        )
            else:
                # position becomes 0 for coin only, usdt position max out
                # quote should be fully spent to buy base for both

                if self.is_reset_condition(market_pair):
                    if self._entry_status.trade_in_progress and self._current_timestamp > (self._last_hedge_placed + 60):
                        if self._reset_balance_when_initializing:
                            self.reset_position(market_pair)
                        self._entry_status.reset_entry_status()
                        self.initialize_order_levels(market_pair)

                if self._logging_options & self.OPTION_LOG_NULL_ORDER_SIZE:
                    self.logger().info(
                        f"Attempting to place a limit buy but the "
                        f"bid size is 0. Skipping. Check available balance."
                    )
        # if there is no active ask, place ask again
        if not has_active_sell:
            sell_size = self.c_get_market_making_size(market_pair, False)
            
            if sell_size > s_decimal_zero:
                sell_price = self.c_get_market_making_price(market_pair, False, sell_size)

                if not Decimal.is_nan(sell_price) and self.is_within_range(market_pair, False, sell_price):
                    effective_hedging_price = self.c_calculate_effective_hedging_price(
                        market_pair,
                        False,
                        sell_size
                    )
                    effective_hedging_price_adjusted = effective_hedging_price / self.market_conversion_rate(True)
                    if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                        self.logger().debug(
                            f"[Create]-Maker-Sell-{sell_size} {market_pair.maker.base_asset}-{sell_price} {market_pair.maker.quote_asset}-"
                            f"hedging price-{effective_hedging_price:.8f} {market_pair.maker.quote_asset}-"
                            f"rate adjusted-{effective_hedging_price_adjusted:.8f} {market_pair.taker.quote_asset}"
                        )
                    order_id = self.c_place_order(market_pair, False, True, sell_size, sell_price)
                else:
                    if self._logging_options & self.OPTION_LOG_NULL_ORDER_SIZE:
                        self.logger().info(
                            f"({market_pair.maker.trading_pair})"
                            f"Order book on taker is too thin to place order for size: {sell_size}"
                            f"Reduce order_size_portfolio_ratio_limit"
                        )
            else:
                if self._logging_options & self.OPTION_LOG_NULL_ORDER_SIZE:
                    self.logger().info(
                        f"Attempting to place a limit sell but the "
                        f"ask size is 0. Skipping. Check available balance."
                    )

    cdef str c_place_order(self,
                           object market_pair,
                           bint is_buy,
                           bint is_maker,  # True for maker order, False for taker order
                           object amount,
                           object price):
        cdef:
            str order_id
            double expiration_seconds = NaN
            object market_info = market_pair.maker if is_maker else market_pair.taker
            object order_type = market_info.market.get_maker_order_type() if is_maker else \
                market_info.market.get_taker_order_type()
        if order_type is OrderType.MARKET:
            price = s_decimal_nan
        if not self._active_order_canceling:
            expiration_seconds = self._limit_order_min_expiration
        
        if is_maker:
            if is_buy:
                order_id = StrategyBase.c_buy_with_specific_market(self, market_info, amount,
                                                                order_type=order_type, price=price,
                                                                expiration_seconds=expiration_seconds)
            else:
                order_id = StrategyBase.c_sell_with_specific_market(self, market_info, amount,
                                                                    order_type=order_type, price=price,
                                                                    expiration_seconds=expiration_seconds)
        else:
            if is_buy:
                order_id = self.buy_with_specific_market(market_info, amount,
                                                    order_type=order_type, price=price, position_action=PositionAction.CLOSE)
            else:
                order_id = self.sell_with_specific_market(market_info, amount,
                                                    order_type=order_type, price=price, position_action=PositionAction.OPEN)
        self._sb_order_tracker.c_add_create_order_pending(order_id)
        self._market_pair_tracker.c_start_tracking_order_id(order_id, market_info.market, market_pair)
        if is_maker:
            self._maker_order_ids.append(order_id)
        return order_id

    cdef c_cancel_order(self, object market_pair, str order_id):
        market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        StrategyBase.c_cancel_order(self, market_trading_pair_tuple, order_id)
    # ----------------------------------------------------------------------------------------------------------
    # </editor-fold>

    # <editor-fold desc="+ Order tracking entry points">
    # Override the stop tracking entry points to include the market pair tracker as well.
    # ----------------------------------------------------------------------------------------------------------
    cdef c_stop_tracking_limit_order(self, object market_trading_pair_tuple, str order_id):
        self._market_pair_tracker.c_stop_tracking_order_id(order_id)
        StrategyBase.c_stop_tracking_limit_order(self, market_trading_pair_tuple, order_id)

    cdef c_stop_tracking_market_order(self, object market_trading_pair_tuple, str order_id):
        self._market_pair_tracker.c_stop_tracking_order_id(order_id)
        StrategyBase.c_stop_tracking_market_order(self, market_trading_pair_tuple, order_id)
    # ----------------------------------------------------------------------------------------------------------
    # </editor-fold>

    # Removes orders from pending_create
    cdef c_did_create_buy_order(self, object order_created_event):
        order_id = order_created_event.order_id
        self._sb_order_tracker.c_remove_create_order_pending(order_id)

    cdef c_did_create_sell_order(self, object order_created_event):
        order_id = order_created_event.order_id
        self._sb_order_tracker.c_remove_create_order_pending(order_id)

    def notify_hb_app(self, msg: str):
        if self._hb_app_notification:
            super().notify_hb_app(msg)
