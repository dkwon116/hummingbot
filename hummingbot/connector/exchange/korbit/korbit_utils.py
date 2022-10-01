from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True


EXAMPLE_PAIR = "BTC-USD"


DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("-0.0005"),
    taker_percent_fee_decimal=Decimal("0.002"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return True
    # return exchange_info.get("type", None) == "spot" and exchange_info.get("enabled", False)


class KorbitConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="korbit", client_data=None)
    korbit_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your KORBIT API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    korbit_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your KORBIT API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "korbit"


KEYS = KorbitConfigMap.construct()


def get_price_increments(price):
    price = float(price)

    if price < 1:
        unit = 0.0001
    elif price < 10:
        unit = 0.001
    elif price < 100:
        unit = 0.01
    elif price < 1000:
        unit = 0.1
    elif price < 5000:
        unit = 1
    elif price < 10000:
        unit = 5
    elif price < 50000:
        unit = 10
    elif price < 100000:
        unit = 50
    elif price < 500000:
        unit = 100
    elif price < 1000000:
        unit = 500
    elif price >= 1000000:
        unit = 1000
    else:
        raise ValueError('Invalid Price')
    return unit
