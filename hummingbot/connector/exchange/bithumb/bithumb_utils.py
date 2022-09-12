from typing import Optional, Union
from datetime import datetime, timezone
from urllib.parse import urlencode
from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-KRW"
HUMMINGBOT_ID_PREFIX = 777
HBOT_BROKER_ID = "HBOT-"

# NDAX fees: https://bithumb.io/fees
# Fees have to be expressed as percent value
DEFAULT_FEES = [0.04, 0.04]


# USE_ETHEREUM_WALLET not required because default value is false
# FEE_TYPE not required because default value is Percentage
# FEE_TOKEN not required because the fee is not flat

QUERY_PARAMS = ['uuids', 'txids', 'identifiers', 'states']

def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    ts_micro_sec: int = get_tracking_nonce()
    return f"{HBOT_BROKER_ID}{ts_micro_sec}"


def rest_api_url(connector_variant_label: Optional[str]) -> str:
    variant = connector_variant_label if connector_variant_label else "bithumb_main"
    return CONSTANTS.REST_URLS.get(variant)


def wss_url(connector_variant_label: Optional[str]) -> str:
    variant = connector_variant_label if connector_variant_label else "bithumb_main"
    return CONSTANTS.WSS_URLS.get(variant)

def get_rest_url(path_url: str) -> str:
    variant = "bithumb_main"
    return f"{CONSTANTS.REST_URLS.get(variant)}{path_url}"

def get_url_with_param(url, params):
    if params:
        query = generate_query(params)
        return f"{url}?{query}"
    else:
        return url

def generate_query(params):
    """
    receive params in json format
    returns query string
    """
    query = urlencode({
        k: v
        for k, v in params.items()
        if k not in QUERY_PARAMS
    })
    for query_param in QUERY_PARAMS:
        if params.get(query_param):
            param = params.pop(query_param)
            params[f"{query_param}"] = param
            query_params = '&'.join([f"{query_param}={q}"for q in param])
            query = f"{query}&{query_params}" if query else query_params
    return query

def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    pair = exchange_trading_pair.split("_")
    base_asset =pair[0]
    quote_asset = pair[1]
    return base_asset.upper() + "-" + quote_asset.upper()


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    pair = hb_trading_pair.split("-")
    base_asset = pair[0]
    quote_asset = pair[1]
    return base_asset.upper() + "_" + quote_asset.upper()

def split_orderbook_to_bidask(bids, asks, timestamp):
    update_id: float = timestamp / 1000
    bids = [[float(unit["price"]), float(unit["quantity"]), update_id] for unit in bids]
    asks = [[float(unit["price"]), float(unit["quantity"]), update_id] for unit in asks]
    return {"asks": asks, "bids": bids}

def convert_orderbook_ws(orderbook, timestamp):
    update_id: float = timestamp / 1000
    bids = [[float(unit["price"]), float(unit["quantity"]), update_id] for unit in orderbook if unit["orderType"] == "bid"]
    asks = [[float(unit["price"]), float(unit["quantity"]), update_id] for unit in orderbook if unit["orderType"] == "ask"]
    return {"asks": asks, "bids": bids}

def dt_to_ts(dt_str):
    dt = datetime.fromisoformat(dt_str)
    # ts = dt.replace(tzinfo=timezone.utc).timestamp()
    return dt.timestamp()

def sanitize_price(price):
    price_inc = get_price_increments(price)
    return round(price / price_inc, 0) * price_inc

def get_price_increments(price):
    """
        [Order price units]
        ~1          : 0.0001
        ~10         : 0.001
        ~100        : 0.01
        ~1,000      : 0.1
        ~5,000      : 1
        ~10,000     : 5
        ~50,000     : 10
        ~100,000    : 50
        ~500,000    : 100
        ~1,000,000  : 500
        +1,000,000  : 1,000
        """

    price = float(price)
    unit = 0.0001
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
    elif price > 1000000:
        unit = 1000
    else:
        raise ValueError('Invalid Price')
    return unit

def get_min_order_size(price):
    """
        [Min order amount]
        ~100        : 10
        ~1,000      : 1
        ~10,000     : 0.1
        ~100,000    : 0.01
        ~1,000,000  : 0.001
        +1,000,000  : 0.0001
        """
    price = float(price)
    amount = 10
    if price < 100:
        amount = 10
    elif price < 1000:
        amount = 1
    elif price < 10000:
        amount = 0.1
    elif price < 100000:
        amount = 0.01
    elif price < 1000000:
        amount = 0.001
    elif price > 1000000:
        amount = 0.0001
    else:
        raise ValueError('Invalid Price')
    return amount

def validate_price(price: Union[int, float, str]) -> float:
    unit = get_price_increments(price)    
    return price - (price % unit)

# KEYS = {
#     "bithumb_api_key":
#         ConfigVar(key="bithumb_api_key",
#                   prompt="Enter your Bithumb API key >>> ",
#                   required_if=using_exchange("bithumb"),
#                   is_secure=True,
#                   is_connect_key=True),
#     "bithumb_secret_key":
#         ConfigVar(key="bithumb_secret_key",
#                   prompt="Enter your Bithumb secret key >>> ",
#                   required_if=using_exchange("bithumb"),
#                   is_secure=True,
#                   is_connect_key=True),
# }


class BithumbConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="bithumb", client_data=None)
    bithumb_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Bithumb API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    bithumb_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Bithumb secret key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "bithumb"


KEYS = BithumbConfigMap.construct()