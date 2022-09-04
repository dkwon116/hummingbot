from typing import (
    List,
    Tuple
)
from decimal import Decimal
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.perp_xemm.perp_xemm_market_pair import CrossExchangeMarketPair
from hummingbot.strategy.perp_xemm.perp_xemm import PerpXEMMStrategy
from hummingbot.strategy.perp_xemm.perp_xemm_config_map import \
    perp_xemm_config_map as perp_xemm_map
from hummingbot.connector.exchange.paper_trade import create_paper_trade_market
from hummingbot.strategy.order_book_asset_price_delegate import OrderBookAssetPriceDelegate
from hummingbot.connector.exchange_base import ExchangeBase


def start(self):
    bot_id = perp_xemm_map.get("bot_id").value
    reset_balance_when_initializing = perp_xemm_map.get("reset_balance_when_initializing").value
    use_min_profit = perp_xemm_map.get("use_min_profit").value
    use_within_range = perp_xemm_map.get("use_within_range").value
    is_grid = perp_xemm_map.get("is_grid").value
    maker_market = perp_xemm_map.get("maker_market").value.lower()
    taker_market = perp_xemm_map.get("taker_market").value.lower()
    raw_maker_trading_pair = perp_xemm_map.get("maker_market_trading_pair").value
    raw_taker_trading_pair = perp_xemm_map.get("taker_market_trading_pair").value
    min_profitability = perp_xemm_map.get("min_profitability").value / Decimal("100")
    order_amount = perp_xemm_map.get("order_amount").value
    strategy_report_interval = global_config_map.get("strategy_report_interval").value
    limit_order_min_expiration = perp_xemm_map.get("limit_order_min_expiration").value
    cancel_order_threshold = perp_xemm_map.get("cancel_order_threshold").value / Decimal("100")
    active_order_canceling = perp_xemm_map.get("active_order_canceling").value
    adjust_order_enabled = perp_xemm_map.get("adjust_order_enabled").value
    top_depth_tolerance = perp_xemm_map.get("top_depth_tolerance").value
    order_size_taker_volume_factor = perp_xemm_map.get("order_size_taker_volume_factor").value / Decimal("100")
    order_size_taker_balance_factor = perp_xemm_map.get("order_size_taker_balance_factor").value / Decimal("100")
    order_size_portfolio_ratio_limit = perp_xemm_map.get("order_size_portfolio_ratio_limit").value / Decimal("100")
    anti_hysteresis_duration = perp_xemm_map.get("anti_hysteresis_duration").value
    use_oracle_conversion_rate = perp_xemm_map.get("use_oracle_conversion_rate").value
    taker_to_maker_base_conversion_rate = perp_xemm_map.get("taker_to_maker_base_conversion_rate").value
    taker_to_maker_quote_conversion_rate = perp_xemm_map.get("taker_to_maker_quote_conversion_rate").value
    slippage_buffer = perp_xemm_map.get("slippage_buffer").value / Decimal("100")

    enable_reg_offset = perp_xemm_map.get("enable_reg_offset").value
    fixed_beta = perp_xemm_map.get("fixed_beta").value
    perpetual_leverage = perp_xemm_map.get("perpetual_leverage").value
    is_coin_marginated = perp_xemm_map.get("is_coin_marginated").value

    ema_length = perp_xemm_map.get("ema_length").value
    trend_sma_length = perp_xemm_map.get("trend_sma_length").value
    # initial_ema = perp_xemm_map.get("initial_ema").value
    # initial_trend_sma = perp_xemm_map.get("initial_trend_sma").value
    std_length = perp_xemm_map.get("std_length").value
    sampling_interval = perp_xemm_map.get("sampling_interval").value
    trend_interval = perp_xemm_map.get("trend_interval").value

    disparity_sensitivity = perp_xemm_map.get("disparity_sensitivity").value
    disparity_factor = perp_xemm_map.get("disparity_factor").value
    std_factor = perp_xemm_map.get("std_factor").value
    trend_factor = perp_xemm_map.get("trend_factor").value

    price_source = perp_xemm_map.get("price_source").value
    price_source_exchange = perp_xemm_map.get("price_source_exchange").value
    price_source_market = perp_xemm_map.get("price_source_market").value

    asset_price_delegate = None
    delegate_for = perp_xemm_map.get("delegate_for").value
    if price_source == "external_market":
        asset_trading_pair: str = price_source_market
        ext_market = create_paper_trade_market(price_source_exchange, [asset_trading_pair])
        self.markets[price_source_exchange]: ExchangeBase = ext_market
        asset_price_delegate = OrderBookAssetPriceDelegate(ext_market, asset_trading_pair)

    sell_quote_threshold = perp_xemm_map.get("sell_quote_threshold").value
    sell_adj_factor = perp_xemm_map.get("sell_adj_factor").value

    sell_profit_factor = perp_xemm_map.get("sell_profit_factor").value
    buy_profit_factor = perp_xemm_map.get("buy_profit_factor").value
    incremental_buy_factor = perp_xemm_map.get("incremental_buy_factor").value

    bo_size = (perp_xemm_map.get("bo_size").value / Decimal("100"), perp_xemm_map.get("up_bo_size").value / Decimal("100"))
    so_size = (perp_xemm_map.get("so_size").value / Decimal("100"), perp_xemm_map.get("up_so_size").value / Decimal("100"))
    so_levels = (perp_xemm_map.get("so_levels").value, perp_xemm_map.get("up_so_levels").value)
    so_volume_factor = (perp_xemm_map.get("so_volume_factor").value, perp_xemm_map.get("up_so_volume_factor").value)
    so_initial_step = (perp_xemm_map.get("so_initial_step").value / Decimal("100"), perp_xemm_map.get("up_so_initial_step").value / Decimal("100"))
    so_step_scale = (perp_xemm_map.get("so_step_scale").value, perp_xemm_map.get("up_so_step_scale").value)

    use_current_entry_status = perp_xemm_map.get("use_current_entry_status").value
    initial_maker_entry_quote = perp_xemm_map.get("initial_maker_entry_quote").value
    initial_maker_entry_base = perp_xemm_map.get("initial_maker_entry_base").value
    initial_taker_entry_quote = perp_xemm_map.get("initial_taker_entry_quote").value
    initial_taker_entry_base = perp_xemm_map.get("initial_taker_entry_base").value
    initial_bo_ratio = perp_xemm_map.get("initial_bo_ratio").value

    # check if top depth tolerance is a list or if trade size override exists
    if isinstance(top_depth_tolerance, list) or "trade_size_override" in perp_xemm_map:
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
        PerpXEMMStrategy.OPTION_LOG_CREATE_ORDER
        | PerpXEMMStrategy.OPTION_LOG_ADJUST_ORDER
        | PerpXEMMStrategy.OPTION_LOG_MAKER_ORDER_FILLED
        | PerpXEMMStrategy.OPTION_LOG_REMOVING_ORDER
        | PerpXEMMStrategy.OPTION_LOG_STATUS_REPORT
        | PerpXEMMStrategy.OPTION_LOG_MAKER_ORDER_HEDGED
    )
    self.strategy = PerpXEMMStrategy()
    self.strategy.init_params(
        market_pairs=[self.market_pair],
        min_profitability=min_profitability,
        status_report_interval=strategy_report_interval,
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
        perp_leverage=perpetual_leverage,
        is_coin_marginated=is_coin_marginated,

        ema_length=ema_length,
        trend_sma_length=trend_sma_length,
        std_length=std_length,
        sampling_interval=sampling_interval,
        trend_interval=trend_interval,

        disparity_sensitivity=disparity_sensitivity,
        disparity_factor=disparity_factor,
        std_factor=std_factor,
        trend_factor=trend_factor,

        asset_price_delegate=asset_price_delegate,
        delegate_for=delegate_for,

        sell_quote_threshold=sell_quote_threshold,
        sell_adj_factor=sell_adj_factor,

        sell_profit_factor=sell_profit_factor,
        buy_profit_factor=buy_profit_factor,
        incremental_buy_factor=incremental_buy_factor,

        bot_id=bot_id,
        reset_balance_when_initializing=reset_balance_when_initializing,
        use_min_profit=use_min_profit,
        use_within_range=use_within_range,
        is_grid=is_grid,

        bo_size = bo_size,
        so_size = so_size,
        so_levels = so_levels,
        so_volume_factor = so_volume_factor,
        so_initial_step = so_initial_step,
        so_step_scale = so_step_scale,

        use_current_entry_status = use_current_entry_status,
        initial_maker_entry_quote = initial_maker_entry_quote,
        initial_maker_entry_base = initial_maker_entry_base,
        initial_taker_entry_quote = initial_taker_entry_quote,
        initial_taker_entry_base = initial_taker_entry_base,
        initial_bo_ratio = initial_bo_ratio,
    )
