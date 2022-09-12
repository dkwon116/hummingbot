import re

import hummingbot.connector.derivative.binance_perp_coin.constants as CONSTANTS

from typing import Optional, Tuple
from decimal import Decimal
from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce


CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0001"),
    taker_percent_fee_decimal=Decimal("0.0005"),
    buy_percent_fee_deducted_from_returns=True
)
SPECIAL_PAIRS = re.compile(r"^(BAT|BNB|HNT|ONT|OXT|USDT|VET)(USD)$")
RE_4_LETTERS_QUOTE = re.compile(r"^(\w{2,})(BIDR|BKRW|BUSD|BVND|IDRT|TUSD|USDC|USDS|USDT)$")
RE_3_LETTERS_QUOTE = re.compile(r"^(\w+)(\w{3})$")


def get_client_order_id(order_side: str, trading_pair: object, broker_id):
    nonce = get_tracking_nonce()
    symbols: str = trading_pair.split("-")
    base: str = symbols[0].upper()
    quote: str = symbols[1].upper()
    return f"{broker_id}-{order_side.upper()[0]}{base[0]}{base[-1]}{quote[0]}{quote[-1]}{nonce}"


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        m = SPECIAL_PAIRS.match(trading_pair)
        if m is None:
            m = RE_4_LETTERS_QUOTE.match(trading_pair)
        if m is None:
            m = RE_3_LETTERS_QUOTE.match(trading_pair)
        return m.group(1), m.group(2)
    # Exceptions are now logged as warnings in trading pair fetcher
    except Exception:
        return None


def convert_from_bifu_coin_to_bifu_usdt(trading_pair: str):
    pair, pair_type = trading_pair.split("_")
    if pair_type == "PERP":
        return pair
    else:
        return None

# def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
#     result = None
#     # exchange_trading_pair = convert_from_bifu_coin_to_bifu_usdt(exchange_trading_pair)
#     splitted_pair = split_trading_pair(exchange_trading_pair)
#     if splitted_pair is not None:
#         # Binance does not split BASEQUOTE (BTCUSDT)
#         base_asset, quote_asset = splitted_pair
#         result = f"{base_asset}-{quote_asset}"
#     return result


def convert_from_exchange_trading_pair(exchange_trading_symbol: str) -> Optional[str]:
    result = None
    exchange_trading_symbol = convert_from_bifu_coin_to_bifu_usdt(exchange_trading_symbol)
    splitted_symbol = split_trading_pair(exchange_trading_symbol)
    if splitted_symbol is not None:
        # Binance does not split BASEQUOTE (BTCUSDT)
        base_asset, quote_asset = splitted_symbol
        result = f"{base_asset}-{quote_asset}"
    return result


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.replace("-", "") +"_PERP"


def rest_url(path_url: str, domain: str = "binance_perp_coin", api_version: str = CONSTANTS.API_VERSION):
    base_url = CONSTANTS.PERPETUAL_BASE_URL if domain == "binance_perp_coin" else CONSTANTS.TESTNET_BASE_URL
    return base_url + api_version + path_url


def wss_url(endpoint: str, domain: str = "binance_perp_coin"):
    base_ws_url = CONSTANTS.PERPETUAL_WS_URL if domain == "binance_perp_coin" else CONSTANTS.TESTNET_WS_URL
    return base_ws_url + endpoint


KEYS = {
    "binance_perp_coin_api_key":
        ConfigVar(key="binance_perp_coin_api_key",
                  prompt="Enter your Binance Perpetual API key >>> ",
                  required_if=using_exchange("binance_perp_coin"),
                  is_secure=True,
                  is_connect_key=True),
    "binance_perp_coin_api_secret":
        ConfigVar(key="binance_perp_coin_api_secret",
                  prompt="Enter your Binance Perpetual API secret >>> ",
                  required_if=using_exchange("binance_perp_coin"),
                  is_secure=True,
                  is_connect_key=True),

}

class BinancePerpCoinConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="binance_perp_coin", client_data=None)
    binance_perp_coin_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance Perpetual Coin API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    binance_perp_coin_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance Perpetual Coin API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )


KEYS = BinancePerpCoinConfigMap.construct()

OTHER_DOMAINS = ["binance_perp_coin_testnet"]
OTHER_DOMAINS_PARAMETER = {"binance_perp_coin_testnet": "binance_perp_coin_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"binance_perp_coin_testnet": "BTC-USDT"}
OTHER_DOMAINS_DEFAULT_FEES = {"binance_perp_coin_testnet": [0.02, 0.04]}
# OTHER_DOMAINS_KEYS = {"binance_perp_coin_testnet": {
#     # add keys for testnet
#     "binance_perp_coin_testnet_api_key":
#         ConfigVar(key="binance_perp_coin_testnet_api_key",
#                   prompt="Enter your Binance Perpetual testnet API key >>> ",
#                   required_if=using_exchange("binance_perp_coin_testnet"),
#                   is_secure=True,
#                   is_connect_key=True),
#     "binance_perp_coin_testnet_api_secret":
#         ConfigVar(key="binance_perp_coin_testnet_api_secret",
#                   prompt="Enter your Binance Perpetual testnet API secret >>> ",
#                   required_if=using_exchange("binance_perp_coin_testnet"),
#                   is_secure=True,
#                   is_connect_key=True),
# }}


class BinancePerpCoinTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="binance_perp_coin_testnet", client_data=None)
    binance_perp_coin_testnet_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance PerpCoin testnet API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    binance_perp_coin_testnet_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance PerpCoin testnet API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "binance_perp_coin"


OTHER_DOMAINS_KEYS = {"binance_perp_coin_testnet": BinancePerpCoinTestnetConfigMap.construct()}