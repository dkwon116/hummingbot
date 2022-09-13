from collections import (
    defaultdict,
    deque
)
from decimal import Decimal
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
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange_base cimport ExchangeBase
from hummingbot.core.data_type.common import OrderType, TradeType, PriceType

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.asset_price_delegate cimport AssetPriceDelegate
from hummingbot.strategy.asset_price_delegate import AssetPriceDelegate
from hummingbot.strategy.order_book_asset_price_delegate cimport OrderBookAssetPriceDelegate

from .cross_exchange_market_pair import CrossExchangeMarketPair
from .order_id_market_pair_tracker import OrderIDMarketPairTracker
from .data_types import (
    Proposal,
    PriceSize
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.client.performance import PerformanceMetrics

from ..__utils__.trailing_indicators.exponential_moving_average import ExponentialMovingAverageIndicator

NaN = float("nan")
s_decimal_zero = Decimal("0")
s_decimal_one = Decimal("1")
s_decimal_nan = Decimal("nan")
s_logger = None


cdef class XEMMStrategy(StrategyBase):
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
                    order_size_taker_volume_factor: Decimal = Decimal("1.0"),
                    order_size_taker_balance_factor: Decimal = Decimal("1.0"),
                    order_size_portfolio_ratio_limit: Decimal = Decimal("1.0"),
                    limit_order_min_expiration: float = 130.0,
                    adjust_order_enabled: bool = True,
                    anti_hysteresis_duration: float = 60.0,
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
                    fixed_beta: Decimal = Decimal("0.02"),

                    disparity_sensitivity: Decimal = Decimal("0.006"),
                    disparity_factor: Decimal = Decimal("0.5"),
                    std_factor: Decimal = Decimal("0.04"),
                    trend_factor: Decimal = Decimal("10.9"),

                    ema_length: int = 205,
                    fast_ema_length: int = 163,
                    std_length: int = 8,
                    sampling_interval: int = 5,
                    initial_ema: Decimal = Decimal("1230"),
                    initial_fast_ema: Decimal = Decimal("1230"),

                    enable_best_price: bool = False,
                    order_levels: int = 1,
                    order_level_spread: Decimal = s_decimal_zero,
                    order_level_amount: Decimal = s_decimal_zero,

                    asset_price_delegate: AssetPriceDelegate = None
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

        self._current_premium = ()
        self._enable_reg_offset = enable_reg_offset
        self._fixed_beta = fixed_beta

        self._ema_length = ema_length
        self._fast_ema_length = fast_ema_length
        self._initial_ema = initial_ema
        self._initial_fast_ema = initial_fast_ema
        self._std_length = std_length
        self._sampling_interval = sampling_interval
        self._ema = ExponentialMovingAverageIndicator(sampling_length=ema_length, processing_length=1)
        self._fast_ema = ExponentialMovingAverageIndicator(sampling_length=fast_ema_length, processing_length=1)
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
        self._enable_best_price = enable_best_price

        self._order_levels = order_levels
        self._buy_levels = order_levels
        self._sell_levels = order_levels
        self._order_level_spread = order_level_spread
        self._order_level_amount = order_level_amount

        self._asset_price_delegate = asset_price_delegate
        self._initial_base_asset = s_decimal_zero

        self._maker_order_ids = []
        cdef:
            list all_markets = list(self._maker_markets | self._taker_markets)

        self.c_add_markets(all_markets)

    @property
    def market_pairs(self):
        return self._market_pairs

    @property
    def order_amount(self):
        return self._order_amount

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
    def active_bids(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return [(market, limit_order) for market, limit_order in self.active_limit_orders if limit_order.is_buy]

    @property
    def active_asks(self) -> List[Tuple[ExchangeBase, LimitOrder]]:
        return [(market, limit_order) for market, limit_order in self.active_limit_orders if not limit_order.is_buy]

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
    def fast_ema_length(self) -> Decimal:
        return self._fast_ema_length

    @fast_ema_length.setter
    def fast_ema_length(self, value):
        self._fast_ema_length = value

    @property
    def std(self):
        return self._std

    @std.setter
    def std(self, indicator: ExponentialMovingAverageIndicator):
        self._std = indicator
    
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
    def fast_ema(self):
        return self._fast_ema

    @fast_ema.setter
    def fast_ema(self, indicator: ExponentialMovingAverageIndicator):
        self._fast_ema = indicator
    
    @property
    def initial_ema(self):
        return self._initial_ema
    
    @property
    def initial_fast_ema(self):
        return self._initial_fast_ema

    @property
    def sampling_interval(self):
        return self._sampling_interval

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
    def enable_best_price(self):
        return self._enable_best_price
    
    @enable_best_price.setter
    def enable_best_price(self, value):
        self._enable_best_price = value

    @property
    def order_levels(self) -> int:
        return self._order_levels

    @order_levels.setter
    def order_levels(self, value: int):
        self._order_levels = value
        self._buy_levels = value
        self._sell_levels = value

    @property
    def buy_levels(self) -> int:
        return self._buy_levels

    @buy_levels.setter
    def buy_levels(self, value: int):
        self._buy_levels = value

    @property
    def sell_levels(self) -> int:
        return self._sell_levels

    @sell_levels.setter
    def sell_levels(self, value: int):
        self._sell_levels = value

    @property
    def order_level_amount(self) -> Decimal:
        return self._order_level_amount

    @order_level_amount.setter
    def order_level_amount(self, value: Decimal):
        self._order_level_amount = value

    @property
    def order_level_spread(self) -> Decimal:
        return self._order_level_spread

    @order_level_spread.setter
    def order_level_spread(self, value: Decimal):
        self._order_level_spread = value
    
    @property
    def asset_price_delegate(self) -> AssetPriceDelegate:
        return self._asset_price_delegate

    @asset_price_delegate.setter
    def asset_price_delegate(self, value):
        self._asset_price_delegate = value

    @logging_options.setter
    def logging_options(self, int64_t logging_options):
        self._logging_options = logging_options

    def convert_ratio(self, ratio):
        market_pairs = list(self.market_pairs.values())[0]
        maker = market_pairs.maker
        taker = market_pairs.taker

        if maker.quote_asset in ("USD", "USDT", "USDC", "BUSD") and taker.quote_asset == "KRW":
            return ratio * 1000000
        else:
            return ratio

    def get_ref_bid_ask_price(self):
        price_provider = self._asset_price_delegate

        bid = price_provider.get_price_by_type(PriceType.BestBid)
        ask = price_provider.get_price_by_type(PriceType.BestAsk)

        return bid, ask

    def get_last_trade_price(self, market_pair):
        maker_last = market_pair.maker.get_price_by_type(PriceType.LastTrade)
        taker_last = market_pair.taker.get_price_by_type(PriceType.LastTrade)
        delegate_last = None

        if self._asset_price_delegate is not None:
            delegate_last = self._asset_price_delegate.get_price_by_type(PriceType.LastTrade)

        return maker_last, taker_last, delegate_last

    def get_taker_to_maker_conversion_rate(self) -> Tuple[str, Decimal, str, Decimal]:
        """
        Find conversion rates from taker market to maker market
        :return: A tuple of quote pair symbol, quote conversion rate source, quote conversion rate,
        base pair symbol, base conversion rate source, base conversion rate
        """
        quote_rate = Decimal("1")
        market_pairs = list(self._market_pairs.values())[0]
        quote_pair = f"{market_pairs.taker.quote_asset}-{market_pairs.maker.quote_asset}"  #KRW-USDT
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

        taker_quote_balance = taker.market.get_available_balance(taker.quote_asset)
        maker_base_balance, maker_quote_balance = self.get_adjusted_available_balance(market_pair)

        total_base = maker.base_balance + taker.base_balance
        quote_in_maker = maker_quote_balance + (taker_quote_balance / taker_bid * maker_ask)
        quote_in_taker = (maker_quote_balance / maker_bid * taker_ask) + taker_quote_balance

        balance_ratio = (total_base * maker_ask) / quote_in_maker

        return total_base, quote_in_maker, quote_in_taker, balance_ratio

    def log_conversion_rates(self, market_pair):
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_taker_to_maker_conversion_rate()
        maker_bid, maker_ask, taker_bid, taker_ask = self.c_get_bid_ask_prices(market_pair)

        if quote_pair.split("-")[0] != quote_pair.split("-")[1]:
            self.logger().info(f"[Rate] {quote_pair}-[conversion]-{PerformanceMetrics.smart_round(quote_rate)}-{self._ema.current_value:.2f}-[maker]-{maker_bid:.8f}-{maker_ask:.8f}-[taker]-{taker_bid:.8f}-{taker_ask:.8f}")
        if base_pair.split("-")[0] != base_pair.split("-")[1]:
            self.logger().info(f"[Rate] {base_pair}-[conversion]-{PerformanceMetrics.smart_round(base_rate)}-{self._ema.current_value:.2f}-[maker]-{maker_bid:.8f}-{maker_ask:.8f}-[taker]-{taker_bid:.8f}-{taker_ask:.8f}")

    def oracle_status_df(self):
        columns = ["Source", "Pair", "Rate"]
        data = []
        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_taker_to_maker_conversion_rate()
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
        market_pair = list(self._market_pairs.values())[0]
        maker_market = market_pair.maker.market
        taker_market = market_pair.taker.market
        # tm, mt = self.c_calculate_premium(market_pair, False)
        adj_tm, adj_mt = self.c_calculate_premium(market_pair, True)
        
        columns = [f"{maker_market.display_name}", f"{taker_market.display_name}", "Adjusted", "Target"]
        data = []
        
        data.extend([
            ["Sell", "Buy", f"{adj_mt:.2%}", f"{self._sell_profit:.2%}"],
            ["Buy", "Sell", f"{adj_tm:.2%}", f"{self._buy_profit:.2%}"],
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

        # if self._entry_status.current_entry_ratio > s_decimal_zero and self._use_min_profit:
        #     entry_ratio = max(self._entry_status.current_entry_ratio, entry_ratio) if not self._is_grid else self._entry_status.current_entry_ratio
        #     entry_target = entry_ratio / ema - 1
        #     entry_price = (taker_hedge_buy_price / self.market_conversion_rate(True)) * Decimal(entry_ratio)
        
        # if self._entry_status.average_ratio > s_decimal_zero and self._use_min_profit:
        #     tp_ratio = min(self.get_min_profit_ratio(), tp_ratio) if not self._is_grid else self.get_min_profit_ratio()
        #     tp_target = 1 - tp_ratio / ema
        #     tp_price = (taker_hedge_sell_price / self.market_conversion_rate(True)) * Decimal(tp_ratio)
        
        data.extend([
            ["Sell", "Buy", f"{adj_mt:.2%}", f"{ema * (1+adj_mt):.2f}", f"{entry_target:.2%}", f"{entry_ratio:.2f}", f"{entry_price:.4f}"],
            ["Buy", "Sell", f"{adj_tm:.2%}", f"{ema / (1+adj_tm):.2f}", f"{tp_target:.2%}", f"{tp_ratio:.2f}", f"{tp_price:.4f}"],
        ])

        return pd.DataFrame(data=data, columns=columns)

    def profit_calc_df(self):
        columns = [f"Side", "Profit", "Disparity", "Trend", "Std"]
        data = []
        self.calculate_profit_factors()
        std = Decimal(str(self._ema.std_dev(self._std_length) / self._ema.current_value))
        data.extend([
            ["Sell", f"{self._sell_profit:.2%}", f"{self._disparity[0]:.4%}", f"{self._trend[0]:.4%}", f"{std:.3%}"],
            ["Buy", f"{self._buy_profit:.2%}", f"{self._disparity[1]:.4%}", f"{self._trend[1]:.4%}", f"{std:.3%}"],
        ])

        return pd.DataFrame(data=data, columns=columns)

    def market_status_data_frame(self, market_trading_pair_tuples) -> pd.DataFrame:
        markets_data = []
        markets_columns = ["Exchange", "Market", "Best Bid", "Best Ask", "Last", "Spread"]

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
                last_price = market.get_price_by_type(trading_pair, PriceType.LastTrade)
                markets_data.append([
                    market.display_name,
                    trading_pair,
                    float(bid_price),
                    float(ask_price),
                    float(last_price),
                    f"{spread:.2%}"
                ])
            return pd.DataFrame(data=markets_data, columns=markets_columns)

        except Exception:
            self.logger().error("Error formatting market stats.", exc_info=True)

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

            warning_lines.extend(self.network_warning([market_pair.maker, market_pair.taker]))

            markets_df = self.market_status_data_frame([market_pair.maker, market_pair.taker])
            maker_last, taker_last, delegate_last = self.get_last_trade_price(market_pair)

            mt_last_ratio = maker_last / taker_last
            dt_last_ratio = delegate_last / taker_last if delegate_last is not None else s_decimal_zero

            lines.extend(["", f"  Markets: MT Ratio {mt_last_ratio:.2f} DT Ratio {dt_last_ratio:.2f}"] +
                         ["    " + line for line in str(markets_df).split("\n")])

            oracle_df = self.oracle_status_df()
            if not oracle_df.empty:
                lines.extend(["", "  Rate conversion:"] +
                             ["    " + line for line in str(oracle_df).split("\n")])
            
            assets_df = self.wallet_balance_data_frame([maker, taker])
            maker_quote = maker.quote_asset
            maker_base = maker.base_asset
            taker_quote = taker.quote_asset

            total_base, quote_in_maker, quote_in_taker, balance_ratio = self.get_total_assets(market_pair)

            lines.extend(["", f"  Assets: Base {total_base:.4f}{maker_base} Maker Quote {quote_in_maker:.2f}{maker_quote} Taker Quote {quote_in_taker:.2f}{taker_quote} Ratio {balance_ratio:.2f}"] +
                         ["    " + line for line in str(assets_df).split("\n")])

            factor_df = self.profit_calc_df()
            if not factor_df.empty:
                lines.extend(["", f"  Profit Factors: {self._ema.current_value:.4f}"] +
                             ["    " + line for line in str(factor_df).split("\n")])


            premium_df = self.premium_status_df()
            prem = Decimal(str(self.convert_ratio(self._ema.current_value)))
            if not premium_df.empty:
                lines.extend(["", f"  Premium: EMA {prem:.2f}"] +
                             ["    " + line for line in str(premium_df).split("\n")])


            # See if there're any open orders.
            if market_pair in tracked_maker_orders and len(tracked_maker_orders[market_pair]) > 0:
                limit_orders = list(tracked_maker_orders[market_pair].values())
                bid, ask = self.c_get_top_bid_ask(market_pair)
                mid_price = (bid + ask)/2

                taker_bid = markets_df["Best Bid"][1]
                taker_ask = markets_df["Best Ask"][1]

                if self._enable_reg_offset:
                    taker_bid *= self._ema.current_value
                    taker_ask *= self._ema.current_value

                df = LimitOrder.to_pandas(limit_orders, mid_price)

                # Add spread to hedge order
                tm_spread = taker_bid / df["Price"]
                mt_spread = df["Price"] / taker_ask
                df["Spread"] = np.where(df["Type"] == "buy", tm_spread, mt_spread)
                df["Spread"] = ((df["Spread"] - 1) * 100).round(2)
                
                df_lines = str(df).split("\n")
                lines.extend(["", "  Active orders:"] +
                             ["    " + line for line in df_lines])
            else:
                lines.extend(["", "  No active maker orders."])

            warning_lines.extend(self.balance_warning([market_pair.maker, market_pair.taker]))

        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    # The following exposed Python functions are meant for unit tests
    # ---------------------------------------------------------------

    def get_order_size_after_portfolio_ratio_limit(self, market_pair: CrossExchangeMarketPair) -> Decimal:
        return self.c_get_order_size_after_portfolio_ratio_limit(market_pair)
    
    def get_top_bid_ask_from_price_samples(self, market_pair: CrossExchangeMarketPair):
        return self.c_get_top_bid_ask_from_price_samples(market_pair)
    
    def get_top_bid_ask(self, market_pair: CrossExchangeMarketPair):
        return self.c_get_top_bid_ask(market_pair)

    def get_market_making_size(self, market_pair: CrossExchangeMarketPair, bint is_bid) -> Decimal:
        return self.c_get_market_making_size(market_pair, is_bid)

    def get_market_making_price(self, market_pair: CrossExchangeMarketPair, bint is_bid, size: Decimal) -> Decimal:
        return self.c_get_market_making_price(market_pair, is_bid, size)

    def get_adjusted_limit_order_size(self, market_pair: CrossExchangeMarketPair) -> Tuple[Decimal, Decimal]:
        return self.c_get_adjusted_limit_order_size(market_pair)

    def get_effective_hedging_price(self, market_pair: CrossExchangeMarketPair, bint is_bid, size: Decimal) -> Decimal:
        return self.c_calculate_effective_hedging_price(market_pair, is_bid, size)

    def check_if_still_profitable(self, market_pair: CrossExchangeMarketPair,
                                  LimitOrder active_order,
                                  current_hedging_price: Decimal) -> bool:
        return self.c_check_if_still_profitable(market_pair, active_order, current_hedging_price)

    def check_if_sufficient_balance(self, market_pair: CrossExchangeMarketPair,
                                    LimitOrder active_order) -> bool:
        return self.c_check_if_sufficient_balance(market_pair, active_order)

    # ---------------------------------------------------------------

    cdef c_start(self, Clock clock, double timestamp):
        StrategyBase.c_start(self, clock, timestamp)
        self._last_timestamp = timestamp

    cdef c_stop(self, Clock clock):
        market_pair = list(self._market_pairs.values())[0]
        total_base, quote_in_maker, quote_in_taker, balance_ratio = self.get_total_assets(market_pair)
        maker_base_balance, maker_quote_balance = self.get_adjusted_available_balance(market_pair)
        self.logger().info(f"Assets at Stop: Base {total_base:.4f} Quote in Maker {quote_in_maker:.2f} Quote in Taker {quote_in_taker:.2f}")
        self.logger().info(f"Maker Assets: Base {maker_base_balance} Quote {maker_quote_balance}")

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
                if self._last_conv_rates_logged + (60. *  self._sampling_interval) < self._current_timestamp:
                    self.log_conversion_rates(market_pair)
                    self._last_conv_rates_logged = self._current_timestamp
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
        self.logger().info(f"Filling sampling buffer EMA filled{self._ema.sampling_length_filled()} Fast EMA Filled {self._fast_ema.sampling_length_filled()}")
        ema_to_fill = self._ema_length - self._ema.sampling_length_filled()
        fast_ema_to_fill = self._fast_ema_length - self._fast_ema.sampling_length_filled()
        std_to_fill = self._std_length - self._std.sampling_length_filled()
        for i in range(ema_to_fill): self._ema.add_sample(self._initial_ema)
        for i in range(fast_ema_to_fill): self._fast_ema.add_sample(self._initial_fast_ema)
        for i in range(std_to_fill): self._std.add_sample(self._initial_ema)

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
            bint has_active_bid = False
            bint has_active_ask = False
            bint need_adjust_order = False
            double anti_hysteresis_timer = self._anti_hysteresis_timers.get(market_pair, 0)

        global s_decimal_zero

        
        if self._last_conv_rates_logged == 0:
            self._initial_base_asset, _, _, _ = self.get_total_assets(market_pair)

        if self._last_conv_rates_logged + (60. * self._sampling_interval) < self._current_timestamp:

            m_bid, m_ask, t_bid, t_ask = self.c_get_bid_ask_prices(market_pair)

            m_last, t_last, d_last = self.get_last_trade_price(market_pair)
            last_ratio = m_last / t_last if self._asset_price_delegate is None else d_last / t_last

            if self._ema.sampling_length_filled() > 0:
                self._ema.add_sample(last_ratio)
                self._fast_ema.add_sample(last_ratio)
                self._std.add_sample(last_ratio)
            else:
                self.fill_sampling_buffer()

        self.c_take_suggested_price_sample(market_pair)

        for active_order in active_orders:
            # Mark the has_active_bid and has_active_ask flags
            is_buy = active_order.is_buy
            if is_buy:
                has_active_bid = True
            else:
                has_active_ask = True

            # Suppose the active order is hedged on the taker market right now, what's the average price the hedge
            # would happen?
            current_hedging_price = self.c_calculate_effective_hedging_price(
                market_pair,
                is_buy,
                active_order.quantity
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
            if self._current_timestamp >= anti_hysteresis_timer:
                # if not self.c_check_if_price_has_drifted(market_pair, active_order):
                self.c_cancel_order(market_pair, active_order.client_order_id)
                continue

        # If there's both an active bid and ask, then there's no need to think about making new limit orders.
        # if has_active_bid and has_active_ask:
        #     return

        # If there are pending taker orders, wait for them to complete
        if self.has_active_taker_order(market_pair):
            return

        # if self._current_timestamp > anti_hysteresis_timer and len(self._sb_order_tracker.in_flight_cancels) == 0 and len(self.active_limit_orders) == 0:
        self.c_check_and_create_new_orders(market_pair, has_active_bid, has_active_ask)
            # self._anti_hysteresis_timers[market_pair] = self._current_timestamp + self._anti_hysteresis_duration


    def log_order_fill_event(self, order_filled_event):
        conversion_rate = self.market_conversion_rate(False)
        
        order_id = order_filled_event.order_id
        market = "Maker" if order_id in self._maker_order_ids else "Taker"
        trade_type = "Buy" if order_filled_event.trade_type is TradeType.BUY else "Sell"
        trading_pair = order_filled_event.trading_pair.split("-")
        base_asset = trading_pair[0]
        quote_asset = trading_pair[1]
        base_amount = Decimal(order_filled_event.amount)
        maker_price = Decimal(order_filled_event.price) if market is "Maker" else Decimal(order_filled_event.price) * conversion_rate
        taker_price = Decimal(order_filled_event.price) if market is "Taker" else Decimal(order_filled_event.price) / conversion_rate
        maker_value = base_amount * maker_price
        taker_value = base_amount * taker_price

        self.logger().info(f"[Filled]-{market}-{trade_type}-{base_amount}-{base_asset}-[Price]-{maker_price:.8f}-{taker_price}-[Value]-{maker_value:.2f}-{taker_value:.2f}-[Rate]-{conversion_rate}")
        
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

        self.log_order_fill_event(order_filled_event)
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
                self.notify_hb_app_with_timestamp(
                    f"Maker buy order ({limit_order_record.quantity} {limit_order_record.base_currency} @ "
                    f"{limit_order_record.price} {limit_order_record.quote_currency}) is filled."
                )
            else:
                self.logger().info(
                    f"Taker buy order {order_id} for "
                    f"({order_completed_event.base_asset_amount} {order_completed_event.base_asset} has been completely filled."
                )
                self.notify_hb_app_with_timestamp(
                    f"Taker buy order {order_completed_event.base_asset_amount} {order_completed_event.base_asset} is filled."
                )

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
                self.notify_hb_app_with_timestamp(
                    f"Maker sell order ({limit_order_record.quantity} {limit_order_record.base_currency} @ "
                    f"{limit_order_record.price} {limit_order_record.quote_currency}) is filled."
                )
            else:
                self.logger().info(
                    f"Taker sell order {order_id} for "
                    f"({order_completed_event.base_asset_amount} {order_completed_event.base_asset} has been completely filled."
                )
                self.notify_hb_app_with_timestamp(
                    f"Taker sell order {order_completed_event.base_asset_amount} {order_completed_event.base_asset} is filled."
                )

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
        
        taker_bid *= Decimal(str(self._ema.current_value)) if is_adjusted and self._enable_reg_offset else self.market_conversion_rate(False)
        taker_ask *= Decimal(str(self._ema.current_value)) if is_adjusted and self._enable_reg_offset else self.market_conversion_rate(False)

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
            taker_top = taker_market.get_price(taker_trading_pair, False)
            hedged_order_quantity = min(
                buy_fill_quantity,
                taker_market.get_available_balance(market_pair.taker.base_asset) 
            )
            size_quantum = get_order_size_quantum(taker_trading_pair, hedged_order_quantity)
            hedged_order_quantity = round(hedged_order_quantity / size_quantum) * size_quantum

            quantized_hedge_amount = taker_market.quantize_order_amount(taker_trading_pair, Decimal(str(hedged_order_quantity)))
            order_price = taker_market.get_price_for_volume(taker_trading_pair, False, quantized_hedge_amount).result_price
            order_price *= Decimal("1") - self._slippage_buffer
            order_price = taker_market.quantize_order_price(taker_trading_pair, min(taker_top, order_price))

            if quantized_hedge_amount > s_decimal_zero:
                hedge_id = self.c_place_order(market_pair, False, False, quantized_hedge_amount, order_price)
                del self._order_fill_buy_events[market_pair]
                if self._logging_options & self.OPTION_LOG_MAKER_ORDER_HEDGED:
                    order_price_converted = order_price * self._fixed_beta
                    avg_buy_price = (sum([r.price * r.amount for _, r in buy_fill_records]) /
                                    sum([r.amount for _, r in buy_fill_records]))
                    self.logger().info(
                        f"[Hedge]-Maker-Buy-{(avg_buy_price/taker_top):.2f}-{buy_fill_quantity} {market_pair.maker.base_asset}-{avg_buy_price}-{(buy_fill_quantity * avg_buy_price):.0f}-"
                        f"Taker-Sell-{quantized_hedge_amount}-{taker_top}-{quantized_hedge_amount * order_price}"
                    )
            else:
                self.logger().info(
                    f"Current maker buy fill amount of "
                    f"{buy_fill_quantity} {market_pair.maker.base_asset} is less than the minimum order amount "
                    f"allowed on the taker market. No hedging possible yet."
                )

        if sell_fill_quantity > 0:
            taker_top = taker_market.get_price(taker_trading_pair, True)
            hedged_order_quantity = min(
                sell_fill_quantity,
                taker_market.get_available_balance(market_pair.taker.quote_asset) /
                market_pair.taker.get_price_for_volume(True, sell_fill_quantity).result_price
            )
            size_quantum = get_order_size_quantum(taker_trading_pair, hedged_order_quantity)
            hedged_order_quantity = round(hedged_order_quantity / size_quantum) * size_quantum
            
            quantized_hedge_amount = taker_market.quantize_order_amount(taker_trading_pair, Decimal(hedged_order_quantity))
            order_price = taker_market.get_price_for_volume(taker_trading_pair, True, quantized_hedge_amount).result_price
            order_price *= Decimal("1") + self._slippage_buffer
            order_price = taker_market.quantize_order_price(taker_trading_pair, max(taker_top, order_price))

            if quantized_hedge_amount > s_decimal_zero:
                hedge_id = self.c_place_order(market_pair, True, False, quantized_hedge_amount, order_price)
                del self._order_fill_sell_events[market_pair]
                if self._logging_options & self.OPTION_LOG_MAKER_ORDER_HEDGED:
                    order_price_converted = order_price * self._fixed_beta
                    avg_sell_price = (sum([r.price * r.amount for _, r in sell_fill_records]) /
                                    sum([r.amount for _, r in sell_fill_records]))
                    self.logger().info(
                        f"[Hedge]-Maker-Sell-{(avg_sell_price/taker_top):.2f}-{sell_fill_quantity} {market_pair.maker.base_asset}-{avg_sell_price}-{(sell_fill_quantity * avg_sell_price):.0f}-"
                        f"Taker-Buy-{quantized_hedge_amount}-{taker_top}-{quantized_hedge_amount * order_price}"
                    )
            else:
                self.logger().info(
                    f"Current maker sell fill amount of "
                    f"{sell_fill_quantity} {market_pair.maker.base_asset} is less than the minimum order amount "
                    f"allowed on the taker market. No hedging possible yet."
                )

    cdef object c_get_adjusted_limit_order_size(self, object market_pair):
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
            object base_balance = maker_market.get_balance(market_pair.maker.base_asset)
            object quote_balance = maker_market.get_balance(market_pair.maker.quote_asset)
            object current_price = (maker_market.get_price(trading_pair, True) +
                                    maker_market.get_price(trading_pair, False)) * Decimal(0.5)
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


        :param market_pair: The cross exchange market pair to calculate order price/size limits.
        :param is_buy: Whether the order to make will be bid or ask.
        :return: a Decimal which is the size of maker order.
        """
        cdef:
            str maker_trading_pair = market_pair.maker.trading_pair
            str taker_trading_pair = market_pair.taker.trading_pair
            ExchangeBase maker_market = market_pair.maker.market
            ExchangeBase taker_market = market_pair.taker.market

        user_order = self.c_get_adjusted_limit_order_size(market_pair)

        if is_buy:
            maker_balance_in_quote = maker_market.get_available_balance(market_pair.maker.quote_asset)

            taker_balance = taker_market.get_available_balance(market_pair.taker.base_asset) * self._order_size_taker_balance_factor

            try:
                # average price to sell user order on taker market
                # taker_price = taker_market.get_vwap_for_volume(taker_trading_pair, False, user_order).result_price
                maker_price = maker_market.get_price_for_quote_volume(
                    maker_trading_pair, True, maker_balance_in_quote
                ).result_price
            except ZeroDivisionError:
                assert user_order == s_decimal_zero
                return s_decimal_zero
            # how much can be bought from maker balance
            # maker_balance = maker_balance_in_quote / (taker_price * self.market_conversion_rate(True))
            maker_balance = maker_balance_in_quote / maker_price * self._order_size_taker_balance_factor

        else:
            maker_balance = maker_market.get_available_balance(market_pair.maker.base_asset) * self._order_size_taker_balance_factor
            taker_balance_in_quote = taker_market.get_available_balance(market_pair.taker.quote_asset)

            try:
                taker_price = taker_market.get_price_for_quote_volume(
                    taker_trading_pair, True, taker_balance_in_quote
                ).result_price
            except ZeroDivisionError:
                assert user_order == s_decimal_zero
                return s_decimal_zero

            taker_slippage_adjustment_factor = Decimal("1") + self._slippage_buffer
            taker_balance = taker_balance_in_quote / (taker_price * taker_slippage_adjustment_factor) * self._order_size_taker_balance_factor
        
        order_amount = min(maker_balance, taker_balance, user_order)
        # order_amount *= Decimal("0.995")

        # make sure order amount selected is also tradeable in taker side
        market_making_size = taker_market.quantize_order_amount(market_pair.taker.trading_pair, order_amount)
        return maker_market.quantize_order_amount(market_pair.maker.trading_pair, market_making_size)
    
    def calculate_profit_factors(self):
        ema = Decimal(str(self._ema.current_value))
        fast_ema = Decimal(str(self._fast_ema.current_value))
        market_pair = list(self._market_pairs.values())[0]
        maker_last = market_pair.maker.get_price_by_type(PriceType.LastTrade)
        taker_last = market_pair.taker.get_price_by_type(PriceType.LastTrade)
        # [0.17, 205, 163, 10.9, 8, 0.04, 0.006, 0.5]
        # [min_spread, ma_length, fast_ma_length, trend_factor, std_length, std_factor, disp_threshold, disp_factor]

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

        # apply trend factor
        trend = Decimal(str(self._fast_ema.trend_slope))
        trend_sell = abs(trend) if trend > 0 else s_decimal_zero
        trend_buy = abs(trend) if trend < 0 else s_decimal_zero
        self._trend = trend_sell, trend_buy

        self._sell_profit = self._min_profitability 
        self._sell_profit += (std * self._std_factor)
        self._sell_profit += (trend_sell * self._trend_factor)
        self._sell_profit += (disp_sell * self._disparity_factor)
        
        self._buy_profit = self._min_profitability 
        self._buy_profit += (std * self._std_factor)
        self._buy_profit += (trend_buy * self._trend_factor)
        self._buy_profit += (disp_buy * self._disparity_factor)

        self._sell_profit = self._min_profitability if Decimal.is_nan(self._sell_profit) else self._sell_profit
        self._buy_profit = self._min_profitability if Decimal.is_nan(self._buy_profit) else self._buy_profit


    def get_profitability(self, market_pair, is_buy):
        # centered around 0 (conversion adjusted price)
        self.calculate_profit_factors()
        if is_buy:
            return max(self._buy_profit, self._min_profitability) if self._buy_profit != s_decimal_nan else self._min_profitability
        else:
            return max(self._sell_profit, self._min_profitability) if self._sell_profit != s_decimal_nan else self._min_profitability

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

        if is_buy:
            if not Decimal.is_nan(sample_top_bid):
                # Calculate the next price above top bid
                price_quantum = maker_market.get_order_price_quantum(
                    market_pair.maker.trading_pair,
                    sample_top_bid
                )
                price_above_bid_sample = (ceil(sample_top_bid / price_quantum) + 1) * price_quantum
                price_above_bid = (ceil(top_bid_price / price_quantum) + 1) * price_quantum

            # you are buying on the maker market and selling on the taker market
            taker_hedge_sell_price = self.c_calculate_effective_hedging_price(market_pair, is_buy, size)
            maker_buy_price = taker_hedge_sell_price / (1 + self.get_profitability(market_pair, is_buy))

            # # If your bid is higher than highest bid price, reduce it to one tick above the top bid price
            if self._adjust_orders_enabled:
                # If maker bid order book is not empty
                if not Decimal.is_nan(price_above_bid_sample):
                    best_bid = price_above_bid if self._enable_best_price else (ceil(top_ask_price / price_quantum) - 1) * price_quantum
                    maker_buy_price = min(maker_buy_price, price_above_bid_sample, best_bid)

            price_quantum = maker_market.get_order_price_quantum(
                market_pair.maker.trading_pair,
                maker_buy_price
            )

            # Rounds down for ensuring profitability
            maker_buy_price = (floor(maker_buy_price / price_quantum)) * price_quantum

            return maker_buy_price
        else:
            if not Decimal.is_nan(sample_top_ask):
                # Calculate the next price below top ask and above top bid
                price_quantum = maker_market.get_order_price_quantum(
                    market_pair.maker.trading_pair,
                    sample_top_ask
                )
                price_below_ask_sample = (floor(sample_top_ask / price_quantum) - 1) * price_quantum
                price_below_ask = (floor(top_ask_price / price_quantum) - 1) * price_quantum

            # You are selling on the maker market and buying on the taker market
            taker_hedge_buy_price = self.c_calculate_effective_hedging_price(market_pair, is_buy, size)
            maker_sell_price = taker_hedge_buy_price * (1 + self.get_profitability(market_pair, is_buy))

            # If your ask is lower than the the top ask, increase it to just one tick below top ask. Sampled top ask can be below maker price (immediate take)
            if self._adjust_orders_enabled:
                # If maker ask order book is not empty
                if not Decimal.is_nan(price_below_ask_sample):
                    best_ask = price_below_ask if self._enable_best_price else (floor(top_bid_price / price_quantum) + 1) * price_quantum
                    maker_sell_price = max(maker_sell_price, price_below_ask_sample, best_ask)

            price_quantum = maker_market.get_order_price_quantum(
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

        try:
            taker_price = taker_market.get_vwap_for_volume(taker_trading_pair, taker_is_buy, size).result_price
        except ZeroDivisionError:
            return None

        # If quote assets are not same, convert them from taker's quote asset to maker's quote asset
        # if market_pair.maker.quote_asset != market_pair.taker.quote_asset:
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
            top_bid_price = maker_market.get_price(trading_pair, False)

            top_ask_price = maker_market.get_price(trading_pair, True)

        else:
            # Use bid entries in maker order book
            top_bid_price = maker_market.get_price_for_volume(trading_pair,
                                                                False,
                                                                self._top_depth_tolerance).result_price

            # Use ask entries in maker order book
            top_ask_price = maker_market.get_price_for_volume(trading_pair,
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
            object user_order = self.c_get_adjusted_limit_order_size(market_pair)
            ExchangeBase maker_market = market_pair.maker.market
            ExchangeBase taker_market = market_pair.taker.market
            object order_size_limit
            str taker_trading_pair = market_pair.taker.trading_pair

        quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate = \
            self.get_taker_to_maker_conversion_rate()

        if is_buy:
            quote_asset_amount = maker_market.get_balance(market_pair.maker.quote_asset)
            base_asset_amount = taker_market.get_balance(market_pair.taker.base_asset) * base_rate
            order_size_limit = min(base_asset_amount, quote_asset_amount / order_price)
        else:
            base_asset_amount = maker_market.get_balance(market_pair.maker.base_asset)
            quote_asset_amount = taker_market.get_balance(market_pair.taker.quote_asset)
            taker_slippage_adjustment_factor = Decimal("1") + self._slippage_buffer
            taker_price = taker_market.get_price_for_quote_volume(
                taker_trading_pair, True, quote_asset_amount
            ).result_price
            adjusted_taker_price = taker_price * taker_slippage_adjustment_factor
            order_size_limit = min(base_asset_amount, quote_asset_amount / adjusted_taker_price)

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
        if is_adjusted and self._enable_reg_offset:
            return Decimal(str(self._ema.current_value))
        else:
            _, _, quote_rate, _, _, base_rate = self.get_taker_to_maker_conversion_rate()
            return quote_rate / base_rate

    def get_proposals(self, market_pair, is_buy, order_price, order_amount):
        proposals = []
        maker = market_pair.maker
        maker_market = maker.market
        price_quantum = maker_market.get_order_price_quantum(market_pair.maker.trading_pair, order_price)

        if is_buy:    
            for level in range(0, self._buy_levels):
                if self._order_level_spread != s_decimal_zero:
                    price = order_price * (Decimal("1") - (level * self._order_level_spread))
                else:
                    price = order_price - (level * price_quantum)
                price = maker_market.quantize_order_price(maker.trading_pair, price)
                size = order_amount + (self._order_level_amount * level)
                size = maker_market.quantize_order_amount(maker.trading_pair, size)
                if size > 0:
                    proposals.append(PriceSize(price, size))
        else:
            for level in range(0, self._sell_levels):
                if self._order_level_spread != s_decimal_zero:
                    price = order_price * (Decimal("1") + (level * self._order_level_spread))
                else:
                    price = order_price + (level * price_quantum)
                price = maker_market.quantize_order_price(maker.trading_pair, price)
                size = order_amount + (self._order_level_amount * level)
                size = maker_market.quantize_order_amount(maker.trading_pair, size)
                if size > 0:
                    proposals.append(PriceSize(price, size))
        return proposals

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

    def apply_budget_constraint(self, market_pair, proposal):
        maker = market_pair.maker
        taker = market_pair.taker
        maker_market = maker.market

        maker_base, maker_quote = self.get_adjusted_available_balance(market_pair)
        taker_base = taker.market.get_available_balance(taker.base_asset) * self._order_size_taker_balance_factor
        taker_quote = taker.market.get_available_balance(taker.quote_asset) * self._order_size_taker_balance_factor

        # minimum balance 
        # maker quote buy > taker base sell: buy at multiple price, sell at vwap price (how much can buy in base)
        # maker base sell > taker quote buy: sell at multiple price, buy at vwap price (how much can sell in base)

        if len(proposal.buys) > 0:
            quote_balance = min(maker_quote / proposal.buys[0].price, taker_base) * proposal.buys[0].price
        
        if len(proposal.sells) > 0:
            # base_balance = min(maker_base * proposal.sells[0].price, taker_quote) / proposal.sells[0].price
            taker_price = taker.market.get_price_for_quote_volume(
                    taker.trading_pair, True, taker_quote
                ).result_price
            base_balance = min(maker_base, taker_quote / taker_price)
            

        for buy in proposal.buys:
            quote_size = buy.size * buy.price

            # Adjust buy order size to use remaining balance if less than the order amount
            if quote_balance < quote_size:
                adjusted_amount = quote_balance / buy.price
                adjusted_amount = maker_market.quantize_order_amount(maker.trading_pair, adjusted_amount)
                buy.size = adjusted_amount
                quote_balance = s_decimal_zero
            elif quote_balance == s_decimal_zero:
                buy.size = s_decimal_zero
            else:
                quote_balance -= quote_size

        proposal.buys = [o for o in proposal.buys if o.size > 0]

        for sell in proposal.sells:
            base_size = sell.size

            # Adjust sell order size to use remaining balance if less than the order amount
            if base_balance < base_size:
                adjusted_amount = maker_market.quantize_order_amount(maker.trading_pair, base_balance)
                sell.size = adjusted_amount
                base_balance = s_decimal_zero
            elif base_balance == s_decimal_zero:
                sell.size = s_decimal_zero
            else:
                base_balance -= base_size

        proposal.sells = [o for o in proposal.sells if o.size > 0]
    
    def execute_orders_proposal(self, market_pair, proposal):
        expiration_seconds = s_decimal_nan
        orders_created = False
        maker = market_pair.maker

        if len(proposal.buys) > 0:
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                price_quote_str = [f"{buy.size.normalize()} {maker.base_asset}, "
                                   f"{buy.price.normalize()} {maker.quote_asset}"
                                   for buy in proposal.buys]
                self.logger().info(
                    f"({maker.trading_pair}) Creating {len(proposal.buys)} bid orders "
                    f"at (Size, Price): {price_quote_str}"
                )
            for idx, buy in enumerate(proposal.buys):
                bid_order_id = self.c_place_order(market_pair, True, True, buy.size, buy.price)
                orders_created = True
        if len(proposal.sells) > 0:
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                price_quote_str = [f"{sell.size.normalize()} {maker.base_asset}, "
                                   f"{sell.price.normalize()} {maker.quote_asset}"
                                   for sell in proposal.sells]
                self.logger().info(
                    f"({maker.trading_pair}) Creating {len(proposal.sells)} ask "
                    f"orders at (Size, Price): {price_quote_str}"
                )
            for idx, sell in enumerate(proposal.sells):
                ask_order_id = self.c_place_order(market_pair, False, True, sell.size, sell.price)
                orders_created = True
        if orders_created:
            self._anti_hysteresis_timers[market_pair] = self._current_timestamp + self._anti_hysteresis_duration

    cdef c_check_and_create_new_orders(self, object market_pair, bint has_active_bid, bint has_active_ask):
        """
        Check and account for all applicable conditions for creating new limit orders (e.g. profitability, what's the
        right price given depth tolerance and transient orders on the market, account balances, etc.), and create new
        limit orders for market making.

        :param market_pair: cross exchange market pair
        :param has_active_bid: True if there's already an active bid on the maker side, False otherwise
        :param has_active_ask: True if there's already an active ask on the maker side, False otherwise
        """
        cdef:
            object effective_hedging_price

        buys = []
        sells = []
        proposal = None
        # if there is no active bid, place bid again
        if not has_active_bid:
            buy_size = self.c_get_market_making_size(market_pair, True)
            
            # check if bid size is greater than maker and taker market minimum order value
            if buy_size > s_decimal_zero:
                buy_price = self.c_get_market_making_price(market_pair, True, buy_size)
                
                if not Decimal.is_nan(buy_price):
                    buys = self.get_proposals(market_pair,True, buy_price, buy_size)
                    self.logger().info(f"Buy Price {buy_price} Proposals {buys}")
                    # effective_hedging_price = self.c_calculate_effective_hedging_price(
                    #     market_pair,
                    #     True,
                    #     buy_size
                    # )
                    # effective_hedging_price_adjusted = effective_hedging_price / self.market_conversion_rate(True)
                    # if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                    #     self.logger().info(
                    #         f"[Create]-Maker-Buy-{buy_size} {market_pair.maker.base_asset}-{buy_price} {market_pair.maker.quote_asset}-"
                    #         f"hedging price-{effective_hedging_price:.8f} {market_pair.maker.quote_asset}-"
                    #         f"rate adjusted-{effective_hedging_price_adjusted:.8f} {market_pair.taker.quote_asset}"
                    #     )
                    # # order_id = self.c_place_order(market_pair, True, True, buy_size, buy_price)
                else:
                    if self._logging_options & self.OPTION_LOG_NULL_ORDER_SIZE:
                        self.logger().info(
                            f"({market_pair.maker.trading_pair})"
                            f"Order book on taker is too thin to place order for size: {buy_size}"
                            f"Reduce order_size_portfolio_ratio_limit"
                        )
            else:
                if self._logging_options & self.OPTION_LOG_NULL_ORDER_SIZE:
                    self.logger().info(
                        f"Attempting to place a limit buy but the "
                        f"bid size is 0. Skipping. Check available balance."
                    )
        # if there is no active ask, place ask again
        if not has_active_ask:
            sell_size = self.c_get_market_making_size(market_pair, False)
            
            if sell_size > s_decimal_zero:
                sell_price = self.c_get_market_making_price(market_pair, False, sell_size)
                
                if not Decimal.is_nan(sell_price):
                    sells = self.get_proposals(market_pair, False, sell_price, sell_size)
                    self.logger().info(f"Sell Price {sell_price} Proposals {sells}")
                    # effective_hedging_price = self.c_calculate_effective_hedging_price(
                    #     market_pair,
                    #     False,
                    #     sell_size
                    # )
                    # effective_hedging_price_adjusted = effective_hedging_price / self.market_conversion_rate(True)
                    # if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                    #     self.logger().info(
                    #         f"[Create]-Maker-Sell-{sell_size} {market_pair.maker.base_asset}-{sell_price} {market_pair.maker.quote_asset}-"
                    #         f"hedging price-{effective_hedging_price:.8f} {market_pair.maker.quote_asset}-"
                    #         f"rate adjusted-{effective_hedging_price_adjusted:.8f} {market_pair.taker.quote_asset}"
                    #     )
                    # # order_id = self.c_place_order(market_pair, False, True, sell_size, sell_price)
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
        
        proposal = Proposal(buys, sells)
        self.apply_budget_constraint(market_pair, proposal)
        self.execute_orders_proposal(market_pair, proposal)

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
        if is_buy:
            order_id = StrategyBase.c_buy_with_specific_market(self, market_info, amount,
                                                               order_type=order_type, price=price,
                                                               expiration_seconds=expiration_seconds)
        else:
            order_id = StrategyBase.c_sell_with_specific_market(self, market_info, amount,
                                                                order_type=order_type, price=price,
                                                                expiration_seconds=expiration_seconds)
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
