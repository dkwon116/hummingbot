from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_exchange,
    validate_market_trading_pair,
    validate_connector,
    validate_derivative,
    validate_decimal,
    validate_int,
    validate_bool
)
from hummingbot.client.config.config_helpers import parse_cvar_value
from hummingbot.client.settings import (
    required_exchanges,
    requried_connector_trading_pairs,
    AllConnectorSettings,
)
import hummingbot.client.settings as settings
from decimal import Decimal
from typing import Optional


def exchange_on_validated(value: str) -> None:
    required_exchanges.add(value)


def maker_trading_pair_prompt():
    maker_market = perp_xemm_config_map.get("maker_market").value
    example = settings.AllConnectorSettings.get_example_pairs().get(maker_market)
    return "Enter the token trading pair you would like to trade on maker market: %s%s >>> " % (
        maker_market,
        f" (e.g. {example})" if example else "",
    )


def taker_trading_pair_prompt():
    taker_market = perp_xemm_config_map.get("taker_market").value
    example = settings.AllConnectorSettings.get_example_pairs().get(taker_market)
    return "Enter the token trading pair you would like to trade on taker market: %s%s >>> " % (
        taker_market,
        f" (e.g. {example})" if example else "",
    )


def top_depth_tolerance_prompt() -> str:
    maker_market = perp_xemm_config_map["maker_market_trading_pair"].value
    base_asset, quote_asset = maker_market.split("-")
    return f"What is your top depth tolerance? (in {base_asset}) >>> "


def order_amount_prompt() -> str:
    trading_pair = perp_xemm_config_map["maker_market_trading_pair"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the amount of {base_asset} per order? >>> "


def update_oracle_settings(value: str):
    c_map = perp_xemm_config_map
    if not (c_map["use_oracle_conversion_rate"].value is not None and
            c_map["maker_market_trading_pair"].value is not None and
            c_map["taker_market_trading_pair"].value is not None):
        return
    use_oracle = parse_cvar_value(c_map["use_oracle_conversion_rate"], c_map["use_oracle_conversion_rate"].value)
    first_base, first_quote = c_map["maker_market_trading_pair"].value.split("-")
    second_base, second_quote = c_map["taker_market_trading_pair"].value.split("-")
    if use_oracle and (first_base != second_base or first_quote != second_quote):
        settings.required_rate_oracle = True
        settings.rate_oracle_pairs = []
        if first_base != second_base:
            settings.rate_oracle_pairs.append(f"{second_base}-{first_base}")
        if first_quote != second_quote:
            settings.rate_oracle_pairs.append(f"{second_quote}-{first_quote}")
    else:
        settings.required_rate_oracle = False
        settings.rate_oracle_pairs = []


def taker_market_prompt() -> str:
    connector = perp_xemm_config_map.get("taker_market").value
    example = AllConnectorSettings.get_example_pairs().get(connector)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (connector, f" (e.g. {example})" if example else "")


def maker_market_prompt() -> str:
    connector = perp_xemm_config_map.get("maker_market").value
    example = AllConnectorSettings.get_example_pairs().get(connector)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (connector, f" (e.g. {example})" if example else "")


# strategy specific validators
def validate_maker_market_trading_pair(value: str) -> Optional[str]:
    maker_market = perp_xemm_config_map.get("maker_market").value
    return validate_market_trading_pair(maker_market, value)


def validate_taker_market_trading_pair(value: str) -> Optional[str]:
    taker_market = perp_xemm_config_map.get("taker_market").value
    return validate_market_trading_pair(taker_market, value)


def taker_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[perp_xemm_config_map["taker_market"].value] = [value]
    update_oracle_settings(value)


def maker_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[perp_xemm_config_map["maker_market"].value] = [value]
    update_oracle_settings(value)


def validate_price_source(value: str) -> Optional[str]:
    if value not in {"current_market", "external_market"}:
        return "Invalid price source type."


def on_validate_price_source(value: str):
    if value != "external_market":
        perp_xemm_config_map["price_source_exchange"].value = None
        perp_xemm_config_map["price_source_market"].value = None
    if value != "custom_api":
        pass
    else:
        perp_xemm_config_map["price_type"].value = "custom"


def price_source_market_prompt() -> str:
    external_market = perp_xemm_config_map.get("price_source_exchange").value
    return f'Enter the token trading pair on {external_market} >>> '


def validate_price_source_exchange(value: str) -> Optional[str]:
    if value == perp_xemm_config_map.get("maker_market").value:
        return "Price source exchange cannot be the same as maker exchange."
    return validate_exchange(value)


def on_validated_price_source_exchange(value: str):
    if value is None:
        perp_xemm_config_map["price_source_market"].value = None


def validate_price_source_market(value: str) -> Optional[str]:
    market = perp_xemm_config_map.get("price_source_exchange").value
    return validate_market_trading_pair(market, value)


def validate_delegate_for(value: str):
    if value not in ["maker", "taker"]:
        return "Delegation need to be for either maker or taker"


perp_xemm_config_map = {
    "strategy": ConfigVar(key="strategy",
                          prompt="",
                          default="perp_xemm"
                          ),
    "taker_market": ConfigVar(
        key="taker_market",
        prompt="Enter a taker perp connector (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "taker_market_trading_pair": ConfigVar(
        key="taker_market_trading_pair",
        prompt=taker_market_prompt,
        prompt_on_new=True,
        validator=validate_taker_market_trading_pair,
        on_validated=taker_market_on_validated),
    "maker_market": ConfigVar(
        key="maker_market",
        prompt="Enter a maker spot connector (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_connector,
        on_validated=exchange_on_validated),
    "maker_market_trading_pair": ConfigVar(
        key="maker_market_trading_pair",
        prompt=maker_market_prompt,
        prompt_on_new=True,
        validator=validate_maker_market_trading_pair,
        on_validated=maker_market_on_validated),
    "adjust_order_enabled": ConfigVar(
        key="adjust_order_enabled",
        prompt="Do you want to enable adjust order? (Yes/No) >>> ",
        default=True,
        type_str="bool",
        validator=validate_bool,
        required_if=lambda: False,
    ),
    "active_order_canceling": ConfigVar(
        key="active_order_canceling",
        prompt="Do you want to enable active order canceling? (Yes/No) >>> ",
        type_str="bool",
        default=True,
        required_if=lambda: False,
        validator=validate_bool,
    ),
    # Setting the default threshold to 0.05 when to active_order_canceling is disabled
    # prevent canceling orders after it has expired
    "cancel_order_threshold": ConfigVar(
        key="cancel_order_threshold",
        prompt="What is the threshold of profitability to cancel a trade? (Enter 1 to indicate 1%) >>> ",
        default=5,
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, min_value=Decimal(-100), max_value=Decimal(100), inclusive=False),
    ),
    "limit_order_min_expiration": ConfigVar(
        key="limit_order_min_expiration",
        prompt="How often do you want limit orders to expire (in seconds)? >>> ",
        default=130.0,
        type_str="float",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False)
    ),
    "top_depth_tolerance": ConfigVar(
        key="top_depth_tolerance",
        prompt=top_depth_tolerance_prompt,
        default=0,
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=True)
    ),
    "anti_hysteresis_duration": ConfigVar(
        key="anti_hysteresis_duration",
        prompt="What is the minimum time interval you want limit orders to be adjusted? (in seconds) >>> ",
        default=15,
        type_str="float",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False)
    ),
    "order_size_taker_volume_factor": ConfigVar(
        key="order_size_taker_volume_factor",
        prompt="What percentage of hedge-able volume would you like to be traded on the taker market? "
               "(Enter 1 to indicate 1%) >>> ",
        default=Decimal("99.99"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False)
    ),
    "order_size_taker_balance_factor": ConfigVar(
        key="order_size_taker_balance_factor",
        prompt="What percentage of asset balance would you like to use for hedging trades on the taker market? "
               "(Enter 1 to indicate 1%) >>> ",
        default=Decimal("99.99"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False)
    ),
    "order_size_portfolio_ratio_limit": ConfigVar(
        key="order_size_portfolio_ratio_limit",
        prompt="What ratio of your total portfolio value would you like to trade on the maker and taker markets? "
               "Enter 50 for 50% >>> ",
        default=Decimal("99.99"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=False)
    ),
    "use_oracle_conversion_rate": ConfigVar(
        key="use_oracle_conversion_rate",
        type_str="bool",
        prompt="Do you want to use rate oracle on unmatched trading pairs? (Yes/No) >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_bool(v),
        on_validated=update_oracle_settings),
    "taker_to_maker_base_conversion_rate": ConfigVar(
        key="taker_to_maker_base_conversion_rate",
        prompt="Enter conversion rate for taker base asset value to maker base asset value, e.g. "
               "if maker base asset is USD and the taker is DAI, 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal"
    ),
    "taker_to_maker_quote_conversion_rate": ConfigVar(
        key="taker_to_maker_quote_conversion_rate",
        prompt="Enter conversion rate for taker quote asset value to maker quote asset value, e.g. "
               "if maker quote asset is USD and the taker is DAI, 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal"
    ),
    "fixed_beta": ConfigVar(
        key="fixed_beta",
        prompt="Beta offset to use (Enter 1 to indicate 1% >>>)",
        default=Decimal("1215"),
        type_str="decimal",
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(3000), inclusive=True)
    ),
    "disparity_sensitivity": ConfigVar(
        key="disparity_sensitivity",
        prompt="Threshold to apply disparity (disp = current ratio / ratio ema) >>> ",
        default=Decimal("0.006"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "disparity_factor": ConfigVar(
        key="disparity_factor",
        prompt="Factor applied to disparity >>> ",
        default=Decimal("0.5"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "std_factor": ConfigVar(
        key="std_factor",
        prompt="Factor applied to Stdev >>> ",
        default=Decimal("0.04"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "trend_factor": ConfigVar(
        key="trend_factor",
        prompt="Factor applied to trend change (fast_ema / fast_ema[2]) >>> ",
        default=Decimal("10.9"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "price_source": ConfigVar(
        key="price_source",
        prompt="Which price source to use? (current_market/external_market) >>> ",
        type_str="str",
        default="current_market",
        validator=validate_price_source,
        on_validated=on_validate_price_source
    ),
    "price_source_exchange": ConfigVar(
        key="price_source_exchange",
        prompt="Enter external price source exchange name >>> ",
        required_if=lambda: perp_xemm_config_map.get("price_source").value == "external_market",
        type_str="str",
        validator=validate_price_source_exchange,
        on_validated=on_validated_price_source_exchange
    ),
    "price_source_market": ConfigVar(
        key="price_source_market",
        prompt=price_source_market_prompt,
        required_if=lambda: perp_xemm_config_map.get("price_source").value == "external_market",
        type_str="str",
        validator=validate_price_source_market
    ),
    "delegate_for": ConfigVar(
        key="delegate_for",
        prompt="Which market is price source delegated for (maker, taker) >>> ",
        type_str="str",
        default="maker",
        required_if=lambda: perp_xemm_config_map.get("price_source").value == "external_market",
        validator=validate_delegate_for,
    ),
    "sell_quote_threshold": ConfigVar(
        key="sell_quote_threshold",
        prompt="Maximum quote ratio to adjust spread (0 ~ 1) >>> ",
        default=Decimal("0.5"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(1), inclusive=True)
    ),
    "sell_adj_factor": ConfigVar(
        key="sell_adj_factor",
        prompt="Factor applied to adjusted spread >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "sell_profit_factor": ConfigVar(
        key="sell_profit_factor",
        prompt="Factor applied to sell spread >>> ",
        default=Decimal("1.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "buy_profit_factor": ConfigVar(
        key="buy_profit_factor",
        prompt="Factor applied to buy spread >>> ",
        default=Decimal("1.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "incremental_buy_factor": ConfigVar(
        key="incremental_buy_factor",
        prompt="Factor added to buy profit factor >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "std_length": ConfigVar(
        key="std_length",
        prompt="Sampling length of Stdev (interval set as 5m)>>> ",
        default=8,
        validator= lambda v: validate_int(v),
        type_str="int",
    ),
    "bot_id": ConfigVar(
        key="bot_id",
        prompt="What is the bot id to be logged on db? >>> ",
        type_str="str",
        default="bot_id"
    ),
    "reset_balance_when_initializing": ConfigVar(
        key="reset_balance_when_initializing",
        prompt="When initializing balance maker and taker (False when releasing position)? >>> ",
        default=True,
        validator=lambda v: validate_bool(v),
        type_str="bool",
    ),
    "use_min_profit": ConfigVar(
        key="use_min_profit",
        prompt="Use entry status to calculate min TP? >>> ",
        default=True,
        validator=lambda v: validate_bool(v),
        type_str="bool",
    ),
    "use_within_range": ConfigVar(
        key="use_within_range",
        prompt="Use order placement for within range only? >>> ",
        default=True,
        validator=lambda v: validate_bool(v),
        type_str="bool",
    ),
    "is_grid": ConfigVar(
        key="is_grid",
        prompt="Run as grid (TP in previous level not average price)? >>> ",
        default=False,
        validator=lambda v: validate_bool(v),
        type_str="bool",
    ),
    "min_profitability": ConfigVar(
        key="min_profitability",
        prompt="What is the minimum profitability for you to make a trade? (Enter 1 to indicate 1%) >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_decimal(v, Decimal(-100), Decimal("100"), inclusive=True),
        type_str="decimal",
    ),
    "order_amount": ConfigVar(
        key="order_amount",
        prompt=order_amount_prompt,
        prompt_on_new=True,
        type_str="decimal",
        validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False),
    ),
    "perpetual_leverage": ConfigVar(
        key="perpetual_leverage",
        prompt="How much leverage would you like to use on the perpetual exchange? (Enter 1 to indicate 1X) >>> ",
        type_str="int",
        default=1,
        validator= lambda v: validate_int(v),
        prompt_on_new=True),
    "slippage_buffer": ConfigVar(
        key="slippage_buffer",
        prompt="How much buffer do you want to add to the price to account for slippage for taker orders "
               "Enter 1 to indicate 1% >>> ",
        prompt_on_new=True,
        default=Decimal("5"),
        type_str="decimal",
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "ema_length": ConfigVar(
        key="ema_length",
        prompt="Sampling length of ema >>> ",
        default=205,
        validator= lambda v: validate_int(v),
        type_str="int",
    ),
    "trend_sma_length": ConfigVar(
        key="trend_sma_length",
        prompt="Sampling length of trend sma >>> ",
        default=200,
        type_str="int",
        validator= lambda v: validate_int(v)
    ),
    "sampling_interval": ConfigVar(
        key="sampling_interval",
        prompt="Sampling interval to collect data in seconds >>> ",
        default=300,
        validator= lambda v: validate_int(v),
        type_str="int",
    ),
    "trend_interval": ConfigVar(
        key="trend_interval",
        prompt="Trend sampling interval to collect data in seconds >>> ",
        default=3600,
        validator= lambda v: validate_int(v),
        type_str="int",
    ),
    # "initial_ema": ConfigVar(
    #     key="initial_ema",
    #     prompt="Initial EMA value to use (get from TV) >>>)",
    #     default=Decimal("1215"),
    #     type_str="decimal",
    #     validator=lambda v: validate_decimal(v, Decimal(0), Decimal(3000), inclusive=True)
    # ),
    # "initial_trend_sma": ConfigVar(
    #     key="initial_trend_sma",
    #     prompt="Initial trend SMA value to use (get from TV) >>>)",
    #     default=Decimal("1215"),
    #     type_str="decimal",
    #     validator=lambda v: validate_decimal(v, Decimal(0), Decimal(3000), inclusive=True)
    # ),
    "enable_reg_offset": ConfigVar(
        key="enable_reg_offset",
        prompt="Use linear regression beta as offset (rolling regression)>>> ",
        default=False,
        validator=lambda v: validate_bool(v),
        type_str="bool",
    ),
    "is_coin_marginated": ConfigVar(
        key="is_coin_marginated",
        prompt="Is it coin marginated? Hedge happens in coin-m with contracts >>> ",
        type_str="bool",
        default=False,
        required_if=lambda: False,
        validator=validate_bool
    ),
    "bo_size": ConfigVar(
        key="bo_size",
        prompt="Size of BO from available base asset in % >>> ",
        default=Decimal("1.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "so_size": ConfigVar(
        key="so_size",
        prompt="Size of first SO from available base asset in % >>> ",
        default=Decimal("2.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "so_levels": ConfigVar(
        key="so_levels",
        prompt="Number of SO levels to place >>> ",
        default=8,
        validator= lambda v: validate_int(v),
        type_str="int",
    ),
    "so_initial_step": ConfigVar(
        key="so_initial_step",
        prompt="Initial deviation of SO from BO in % >>> ",
        default=Decimal("0.7"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "so_volume_factor": ConfigVar(
        key="so_volume_factor",
        prompt="Factor appied to SO to scale >>> ",
        default=Decimal("1.5"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "so_step_scale": ConfigVar(
        key="so_step_scale",
        prompt="SO scale factor to apply >>> ",
        default=Decimal("1.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "up_bo_size": ConfigVar(
        key="up_bo_size",
        prompt="Size of BO from available base asset in % >>> ",
        default=Decimal("1.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "up_so_size": ConfigVar(
        key="up_so_size",
        prompt="Size of first SO from available base asset in % >>> ",
        default=Decimal("2.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(100), inclusive=True)
    ),
    "up_so_levels": ConfigVar(
        key="up_so_levels",
        prompt="Number of SO levels to place >>> ",
        default=8,
        validator= lambda v: validate_int(v),
        type_str="int",
    ),
    "up_so_initial_step": ConfigVar(
        key="up_so_initial_step",
        prompt="Initial deviation of SO from BO in % >>> ",
        default=Decimal("0.7"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "up_so_volume_factor": ConfigVar(
        key="up_so_volume_factor",
        prompt="Factor appied to SO to scale >>> ",
        default=Decimal("1.5"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "up_so_step_scale": ConfigVar(
        key="up_so_step_scale",
        prompt="SO scale factor to apply >>> ",
        default=Decimal("1.0"),
        type_str="decimal",
        required_if=lambda: False,
        validator=lambda v: validate_decimal(v, Decimal(0), Decimal(10), inclusive=True)
    ),
    "use_current_entry_status": ConfigVar(
        key="use_current_entry_status",
        prompt="Trade in progress and set maker taker value> >>> ",
        type_str="bool",
        default=False,
        required_if=lambda: False,
        validator=validate_bool,
    ),
    "initial_maker_entry_quote": ConfigVar(
        key="initial_maker_entry_quote",
        prompt="Initial maker entry quote value if resuming >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: perp_xemm_config_map.get("use_current_entry_status").value,
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=True)
    ),
    "initial_maker_entry_base": ConfigVar(
        key="initial_maker_entry_base",
        prompt="Initial maker entry base value if resuming >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: perp_xemm_config_map.get("use_current_entry_status").value,
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=True)
    ),
    "initial_taker_entry_quote": ConfigVar(
        key="initial_taker_entry_quote",
        prompt="Initial taker entry quote value if resuming >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: perp_xemm_config_map.get("use_current_entry_status").value,
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=True)
    ),
    "initial_taker_entry_base": ConfigVar(
        key="initial_taker_entry_base",
        prompt="Initial maker entry base value if resuming >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: perp_xemm_config_map.get("use_current_entry_status").value,
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=True)
    ),
    "initial_bo_ratio": ConfigVar(
        key="initial_bo_ratio",
        prompt="Initial BO ratio to use if maker filled base order >>> ",
        default=Decimal("0.0"),
        type_str="decimal",
        required_if=lambda: perp_xemm_config_map.get("use_current_entry_status").value,
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=True)
    )
}
