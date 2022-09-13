from typing import (
    List,
    Tuple
)
from decimal import Decimal
# from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.xemm.cross_exchange_market_pair import CrossExchangeMarketPair
from hummingbot.strategy.xemm.xemm import XEMMStrategy
from hummingbot.strategy.xemm.xemm_config_map import \
    xemm_config_map as xemm_map

from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
from hummingbot.strategy.order_book_asset_price_delegate import OrderBookAssetPriceDelegate
from hummingbot.connector.exchange_base import ExchangeBase


def start(self):
    maker_market = xemm_map.get("maker_market").value.lower()
    taker_market = xemm_map.get("taker_market").value.lower()
    raw_maker_trading_pair = xemm_map.get("maker_market_trading_pair").value
    raw_taker_trading_pair = xemm_map.get("taker_market_trading_pair").value
    min_profitability = xemm_map.get("min_profitability").value / Decimal("100")
    order_amount = xemm_map.get("order_amount").value
    # strategy_report_interval = global_config_map.get("strategy_report_interval").value
    limit_order_min_expiration = xemm_map.get("limit_order_min_expiration").value
    cancel_order_threshold = xemm_map.get("cancel_order_threshold").value / Decimal("100")
    active_order_canceling = xemm_map.get("active_order_canceling").value
    adjust_order_enabled = xemm_map.get("adjust_order_enabled").value
    top_depth_tolerance = xemm_map.get("top_depth_tolerance").value
    order_size_taker_volume_factor = xemm_map.get("order_size_taker_volume_factor").value / Decimal("100")
    order_size_taker_balance_factor = xemm_map.get("order_size_taker_balance_factor").value / Decimal("100")
    order_size_portfolio_ratio_limit = xemm_map.get("order_size_portfolio_ratio_limit").value / Decimal("100")
    anti_hysteresis_duration = xemm_map.get("anti_hysteresis_duration").value
    use_oracle_conversion_rate = xemm_map.get("use_oracle_conversion_rate").value
    taker_to_maker_base_conversion_rate = xemm_map.get("taker_to_maker_base_conversion_rate").value
    taker_to_maker_quote_conversion_rate = xemm_map.get("taker_to_maker_quote_conversion_rate").value
    slippage_buffer = xemm_map.get("slippage_buffer").value / Decimal("100")
    
    enable_reg_offset = xemm_map["enable_reg_offset"].value
    fixed_beta = xemm_map.get("fixed_beta").value / Decimal("100")

    ema_length = xemm_map.get("ema_length").value
    fast_ema_length = xemm_map.get("fast_ema_length").value
    initial_ema = xemm_map.get("initial_ema").value
    initial_fast_ema = xemm_map.get("initial_fast_ema").value
    std_length = xemm_map.get("std_length").value
    sampling_interval = xemm_map.get("sampling_interval").value

    disparity_sensitivity = xemm_map.get("disparity_sensitivity").value
    disparity_factor = xemm_map.get("disparity_factor").value
    std_factor = xemm_map.get("std_factor").value
    trend_factor = xemm_map.get("trend_factor").value
    enable_best_price = xemm_map.get("enable_best_price").value
    order_levels = xemm_map.get("order_levels").value
    order_level_amount = xemm_map.get("order_level_amount").value
    order_level_spread = xemm_map.get("order_level_spread").value / Decimal('100')

    price_source = xemm_map.get("price_source").value
    price_source_exchange = xemm_map.get("price_source_exchange").value
    price_source_market = xemm_map.get("price_source_market").value

    asset_price_delegate = None
    if price_source == "external_market":
        asset_trading_pair: str = price_source_market
        ext_market = create_paper_trade_market(price_source_exchange, self.client_config_map, [asset_trading_pair])
        self.markets[price_source_exchange]: ExchangeBase = ext_market
        asset_price_delegate = OrderBookAssetPriceDelegate(ext_market, asset_trading_pair)

    # check if top depth tolerance is a list or if trade size override exists
    if isinstance(top_depth_tolerance, list) or "trade_size_override" in xemm_map:
        self._notify("Current config is not compatible with cross exchange market making strategy. Please reconfigure")
        return

    try:
        maker_trading_pair: str = raw_maker_trading_pair
        taker_trading_pair: str = raw_taker_trading_pair
        maker_assets: Tuple[str, str] = self._initialize_market_assets(maker_market, [maker_trading_pair])[0]
        taker_assets: Tuple[str, str] = self._initialize_market_assets(taker_market, [taker_trading_pair])[0]
    except ValueError as e:
        self._notify(str(e))
        return

    market_names: List[Tuple[str, List[str]]] = [
        (maker_market, [maker_trading_pair]),
        (taker_market, [taker_trading_pair]),
    ]

    self._initialize_markets(market_names)
    maker_data = [self.markets[maker_market], maker_trading_pair] + list(maker_assets)
    taker_data = [self.markets[taker_market], taker_trading_pair] + list(taker_assets)
    maker_market_trading_pair_tuple = MarketTradingPairTuple(*maker_data)
    taker_market_trading_pair_tuple = MarketTradingPairTuple(*taker_data)
    self.market_trading_pair_tuples = [maker_market_trading_pair_tuple, taker_market_trading_pair_tuple]
    self.market_pair = CrossExchangeMarketPair(maker=maker_market_trading_pair_tuple, taker=taker_market_trading_pair_tuple)

    strategy_logging_options = (
        XEMMStrategy.OPTION_LOG_CREATE_ORDER
        | XEMMStrategy.OPTION_LOG_ADJUST_ORDER
        | XEMMStrategy.OPTION_LOG_MAKER_ORDER_FILLED
        | XEMMStrategy.OPTION_LOG_REMOVING_ORDER
        | XEMMStrategy.OPTION_LOG_STATUS_REPORT
        | XEMMStrategy.OPTION_LOG_MAKER_ORDER_HEDGED
    )
    self.strategy = XEMMStrategy()
    self.strategy.init_params(
        market_pairs=[self.market_pair],
        min_profitability=min_profitability,
        logging_options=strategy_logging_options,
        order_amount=order_amount,
        limit_order_min_expiration=limit_order_min_expiration,
        cancel_order_threshold=cancel_order_threshold,
        active_order_canceling=active_order_canceling,
        adjust_order_enabled=adjust_order_enabled,
        top_depth_tolerance=top_depth_tolerance,
        order_size_taker_volume_factor=order_size_taker_volume_factor,
        order_size_taker_balance_factor=order_size_taker_balance_factor,
        order_size_portfolio_ratio_limit=order_size_portfolio_ratio_limit,
        anti_hysteresis_duration=anti_hysteresis_duration,
        use_oracle_conversion_rate=use_oracle_conversion_rate,
        taker_to_maker_base_conversion_rate=taker_to_maker_base_conversion_rate,
        taker_to_maker_quote_conversion_rate=taker_to_maker_quote_conversion_rate,
        slippage_buffer=slippage_buffer,
        hb_app_notification=True,
        enable_reg_offset=enable_reg_offset,
        fixed_beta=fixed_beta,

        ema_length=ema_length,
        fast_ema_length=fast_ema_length,
        initial_ema=initial_ema,
        initial_fast_ema=initial_fast_ema,
        std_length=std_length,
        sampling_interval=sampling_interval,

        disparity_sensitivity=disparity_sensitivity,
        disparity_factor=disparity_factor,
        std_factor=std_factor,
        trend_factor=trend_factor,
        enable_best_price=enable_best_price,
        order_levels=order_levels,
        order_level_spread=order_level_spread,
        order_level_amount=order_level_amount,
        
        asset_price_delegate=asset_price_delegate,
    )
