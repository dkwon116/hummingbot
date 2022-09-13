# distutils: language=c++

from libc.stdint cimport int64_t
from hummingbot.core.data_type.limit_order cimport LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from .order_id_market_pair_tracker cimport OrderIDMarketPairTracker

cdef class XEMMStrategy(StrategyBase):
    cdef:
        set _maker_markets
        set _taker_markets
        bint _all_markets_ready
        bint _active_order_canceling
        bint _adjust_orders_enabled
        dict _anti_hysteresis_timers
        object _min_profitability
        object _order_size_taker_volume_factor
        object _order_size_taker_balance_factor
        object _order_size_portfolio_ratio_limit
        object _order_amount
        object _cancel_order_threshold
        object _top_depth_tolerance
        double _anti_hysteresis_duration
        double _status_report_interval
        double _last_timestamp
        double _limit_order_min_expiration
        dict _order_fill_buy_events
        dict _order_fill_sell_events
        dict _suggested_price_samples
        dict _market_pairs
        int64_t _logging_options
        OrderIDMarketPairTracker _market_pair_tracker
        bint _use_oracle_conversion_rate
        object _taker_to_maker_base_conversion_rate
        object _taker_to_maker_quote_conversion_rate
        object _slippage_buffer
        bint _hb_app_notification
        list _maker_order_ids
        double _last_conv_rates_logged
        tuple _current_premium

        bint _enable_reg_offset
        object _fixed_beta

        object _ema
        object _fast_ema
        object _std

        object _ema_length
        object _fast_ema_length
        object _initial_ema
        object _initial_fast_ema
        object _std_length
        object _sampling_interval
        
        object _last_price_ratio
        tuple _disparity
        tuple _trend
        object _disparity_sensitivity
        object _disparity_factor
        object _std_factor
        object _trend_factor

        object _buy_profit
        object _sell_profit

        bint _enable_best_price
        int _order_levels
        int _buy_levels
        int _sell_levels
        object _order_level_spread
        object _order_level_amount

        object _asset_price_delegate

    cdef c_process_market_pair(self,
                               object market_pair,
                               list active_ddex_orders)
    cdef c_get_bid_ask_prices(self,
                                object market_pair)
    cdef c_calculate_premium(self,
                            object market_pair,
                            bint is_adjusted)
    cdef c_check_and_hedge_orders(self,
                                  object market_pair)
    cdef object c_get_order_size_after_portfolio_ratio_limit(self,
                                                             object market_pair)
    cdef object c_get_adjusted_limit_order_size(self,
                                                object market_pair)
    cdef object c_get_market_making_size(self,
                                         object market_pair,
                                         bint is_bid)
    cdef object c_get_market_making_price(self,
                                          object market_pair,
                                          bint is_bid,
                                          object size)
    cdef object c_calculate_effective_hedging_price(self,
                                                    object market_pair,
                                                    bint is_bid,
                                                    object size)
    cdef bint c_check_if_still_profitable(self,
                                          object market_pair,
                                          LimitOrder active_order,
                                          object current_hedging_price)
    cdef bint c_check_if_sufficient_balance(self,
                                            object market_pair,
                                            LimitOrder active_order)

    cdef bint c_check_if_price_has_drifted(self,
                                           object market_pair,
                                           LimitOrder active_order)

    cdef tuple c_get_top_bid_ask(self,
                                 object market_pair)
    cdef tuple c_get_top_bid_ask_from_price_samples(self,
                                                    object market_pair)
    cdef tuple c_get_suggested_price_samples(self,
                                             object market_pair)
    cdef c_take_suggested_price_sample(self,
                                       object market_pair)
    cdef c_check_and_create_new_orders(self,
                                       object market_pair,
                                       bint has_active_bid,
                                       bint has_active_ask)
    cdef str c_place_order(self,
                           object market_pair,
                           bint is_buy,
                           bint is_maker,
                           object amount,
                           object price)
