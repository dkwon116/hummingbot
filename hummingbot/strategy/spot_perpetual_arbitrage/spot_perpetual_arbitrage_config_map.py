from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_market_trading_pair,
    validate_connector,
    validate_derivative,
    validate_decimal,
    validate_int,
    validate_bool
)
from hummingbot.client.settings import (
    required_exchanges,
    requried_connector_trading_pairs,
    AllConnectorSettings,
)
import hummingbot.client.settings as settings
from decimal import Decimal
from hummingbot.client.config.config_helpers import parse_cvar_value


def exchange_on_validated(value: str) -> None:
    required_exchanges.append(value)


def spot_market_validator(value: str) -> None:
    exchange = spot_perpetual_arbitrage_config_map["spot_connector"].value
    return validate_market_trading_pair(exchange, value)


def spot_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[spot_perpetual_arbitrage_config_map["spot_connector"].value] = [value]


def perpetual_market_validator(value: str) -> None:
    exchange = spot_perpetual_arbitrage_config_map["perpetual_connector"].value
    return validate_market_trading_pair(exchange, value)


def perpetual_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[spot_perpetual_arbitrage_config_map["perpetual_connector"].value] = [value]


def spot_market_prompt() -> str:
    connector = spot_perpetual_arbitrage_config_map.get("spot_connector").value
    example = AllConnectorSettings.get_example_pairs().get(connector)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (connector, f" (e.g. {example})" if example else "")


def perpetual_market_prompt() -> str:
    connector = spot_perpetual_arbitrage_config_map.get("perpetual_connector").value
    example = AllConnectorSettings.get_example_pairs().get(connector)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (connector, f" (e.g. {example})" if example else "")


def order_amount_prompt() -> str:
    trading_pair = spot_perpetual_arbitrage_config_map["spot_market"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the amount of {base_asset} per order? >>> "


def update_oracle_settings(value: str):
    c_map = spot_perpetual_arbitrage_config_map
    if not (c_map["use_oracle_conversion_rate"].value is not None and
            c_map["spot_market"].value is not None and
            c_map["perpetual_market"].value is not None):
        return
    use_oracle = parse_cvar_value(c_map["use_oracle_conversion_rate"], c_map["use_oracle_conversion_rate"].value)
    first_base, first_quote = c_map["spot_market"].value.split("-")
    second_base, second_quote = c_map["perpetual_market"].value.split("-")
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


spot_perpetual_arbitrage_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="spot_perpetual_arbitrage"),
    "spot_connector": ConfigVar(
        key="spot_connector",
        prompt="Enter a spot connector (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_connector,
        on_validated=exchange_on_validated),
    "spot_market": ConfigVar(
        key="spot_market",
        prompt=spot_market_prompt,
        prompt_on_new=True,
        validator=spot_market_validator,
        on_validated=spot_market_on_validated),
    "perpetual_connector": ConfigVar(
        key="perpetual_connector",
        prompt="Enter a derivative name (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "perpetual_market": ConfigVar(
        key="perpetual_market",
        prompt=perpetual_market_prompt,
        prompt_on_new=True,
        validator=perpetual_market_validator,
        on_validated=perpetual_market_on_validated),
    "order_amount": ConfigVar(
        key="order_amount",
        prompt=order_amount_prompt,
        type_str="decimal",
        prompt_on_new=True),
    "perpetual_leverage": ConfigVar(
        key="perpetual_leverage",
        prompt="How much leverage would you like to use on the perpetual exchange? (Enter 1 to indicate 1X) >>> ",
        type_str="int",
        default=1,
        validator= lambda v: validate_int(v),
        prompt_on_new=True),
    "min_opening_arbitrage_pct": ConfigVar(
        key="min_opening_arbitrage_pct",
        prompt="What is the minimum arbitrage percentage between the spot and perpetual market price before opening "
               "an arbitrage position? (Enter 1 to indicate 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(-100), 100, inclusive=False),
        type_str="decimal"),
    "min_closing_arbitrage_pct": ConfigVar(
        key="min_closing_arbitrage_pct",
        prompt="What is the minimum arbitrage percentage between the spot and perpetual market price before closing "
               "an existing arbitrage position? (Enter 1 to indicate 1%) (This can be negative value to close out the "
               "position with lesser profit at higher chance of closing) >>> ",
        prompt_on_new=True,
        default=Decimal("-0.1"),
        validator=lambda v: validate_decimal(v, Decimal(-100), 100, inclusive=False),
        type_str="decimal"),
    "spot_market_slippage_buffer": ConfigVar(
        key="spot_market_slippage_buffer",
        prompt="How much buffer do you want to add to the price to account for slippage for orders on the spot market "
               "(Enter 1 for 1%)? >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "perpetual_market_slippage_buffer": ConfigVar(
        key="perpetual_market_slippage_buffer",
        prompt="How much buffer do you want to add to the price to account for slippage for orders on the perpetual "
               "market (Enter 1 for 1%)? >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "next_arbitrage_opening_delay": ConfigVar(
        key="next_arbitrage_opening_delay",
        prompt="How long do you want the strategy to wait before opening the next arbitrage position (in seconds)?",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False),
        default=120),
    "use_oracle_conversion_rate": ConfigVar(
        key="use_oracle_conversion_rate",
        type_str="bool",
        prompt="Do you want to use rate oracle on unmatched trading pairs? (Yes/No) >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_bool(v),
        on_validated=update_oracle_settings),
    "perp_to_spot_base_conversion_rate": ConfigVar(
        key="perp_to_spot_base_conversion_rate",
        prompt="Enter conversion rate for taker base asset value to maker base asset value, e.g. "
               "if maker base asset is USD and the taker is DAI, 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal"
    ),
    "perp_to_spot_quote_conversion_rate": ConfigVar(
        key="perp_to_spot_quote_conversion_rate",
        prompt="Enter conversion rate for taker quote asset value to maker quote asset value, e.g. "
               "if maker quote asset is USD and the taker is DAI, 1 DAI is valued at 1.25 USD, "
               "the conversion rate is 1.25 >>> ",
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(0), inclusive=False),
        type_str="decimal"
    ),
    "reg_buffer_size": ConfigVar(
        key="reg_buffer_size",
        prompt="Sampling length of regression (interval set as 5m)>>> ",
        default=12,
        validator=lambda v: validate_decimal(v, 1, 10000),
        type_str="int",
    ),
    "enable_reg_offset": ConfigVar(
        key="enable_reg_offset",
        prompt="Use linear regression beta as offset (rolling regression)>>> ",
        default=False,
        validator=lambda v: validate_bool(v),
        type_str="bool",
    ),
    "fixed_beta": ConfigVar(
        key="fixed_beta",
        prompt="Beta offset to use (Enter 1 to indicate 1% >>>)",
        default=Decimal("2"),
        type_str="decimal",
        validator=lambda v: validate_decimal(v, Decimal(-100), Decimal(100), inclusive=True)
    ),
    "min_buffer_sample": ConfigVar(
        key="min_buffer_sample",
        prompt="Minimum buffer samples needed before using regression result (use fixed beta until) >>>",
        default=144,
        validator=lambda v: validate_decimal(v, 1, 10000),
        type_str="int",
    )
}
