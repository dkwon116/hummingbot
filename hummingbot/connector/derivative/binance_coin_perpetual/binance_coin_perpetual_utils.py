import os
import socket
from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0001"),
    taker_percent_fee_decimal=Decimal("0.0005"),
    buy_percent_fee_deducted_from_returns=True
)


CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USD"


def get_client_order_id(order_side: str, trading_pair: object, broker_id):
    nonce = get_tracking_nonce()
    symbols: str = trading_pair.split("-")
    base: str = symbols[0].upper()
    quote: str = symbols[1].upper()
    return f"{broker_id}-{order_side.upper()[0]}{base[0]}{base[-1]}{quote[0]}{quote[-1]}{nonce}"


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("contractStatus", None) == "TRADING" and exchange_info.get("contractType", None) == "PERPETUAL"


class BinanceCoinPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="binance_coin_perpetual", client_data=None)
    binance_coin_perpetual_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance Coin Perpetual API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    binance_coin_perpetual_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance Coin Perpetual API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )


KEYS = BinanceCoinPerpetualConfigMap.construct()

OTHER_DOMAINS = ["binance_coin_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"binance_coin_perpetual_testnet": "binance_coin_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"binance_coin_perpetual_testnet": "BTC-USD"}
OTHER_DOMAINS_DEFAULT_FEES = {"binance_coin_perpetual_testnet": [0.02, 0.04]}


class BinanceCoinPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="binance_coin_perpetual_testnet", client_data=None)
    binance_coin_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance Coin Perpetual testnet API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    binance_coin_perpetual_testnet_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Binance Coin Perpetual testnet API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "binance_coin_perpetual"


OTHER_DOMAINS_KEYS = {"binance_coin_perpetual_testnet": BinanceCoinPerpetualTestnetConfigMap.construct()}
