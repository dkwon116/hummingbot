# A single source of truth for constant variables related to the exchange
from hummingbot.core.api_throttler.data_types import RateLimit, LinkedLimitWeightPair

EXCHANGE_NAME = "upbit"

REST_URLS = {"upbit_main": "https://api.upbit.com/v1/"}
WSS_URLS = {"upbit_main": "wss://api.upbit.com/websocket/v1"}

REST_API_VERSION = "v1.3"

# REST API Public Endpoints
GET_MARKETS_URL = "market/all"
GET_TICKER_URL = "ticker"
GET_ORDER_BOOK_URL = "orderbook"

# REST API ENDPOINTS
GET_TRADING_RULES_PATH_URL = "orders/chance"
CREATE_ORDER_PATH_URL = "orders"
CANCEL_ORDER_PATH_URL = "order"
GET_ORDER_DETAIL_PATH_URL = "order"
GET_OPEN_ORDERS_PATH_URL = "orders"
GET_ACCOUNT_SUMMARY_PATH_URL = "accounts"


# WebSocket Public Endpoints
WS_PING_REQUEST = "Ping"
WS_PING_ID = "WSPing"

API_LIMIT_REACHED_ERROR_MESSAGE = "TOO MANY REQUESTS"

MINUTE = 60
HTTP_ENDPOINTS_LIMIT_ID = "AllHTTP"
HTTP_LIMIT = 600
WS_AUTH_LIMIT_ID = "AllWsAuth"
WS_ENDPOINTS_LIMIT_ID = "AllWs"
WS_LIMIT = 500
RATE_LIMITS = [
    RateLimit(limit_id=HTTP_ENDPOINTS_LIMIT_ID, limit=HTTP_LIMIT, time_interval=MINUTE),
    # public http
    RateLimit(
        limit_id=GET_MARKETS_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_ORDER_BOOK_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TICKER_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    # private http
    RateLimit(
        limit_id=GET_ACCOUNT_SUMMARY_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TRADING_RULES_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CREATE_ORDER_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_ORDER_DETAIL_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_OPEN_ORDERS_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    # ws public
    RateLimit(limit_id=WS_AUTH_LIMIT_ID, limit=50, time_interval=MINUTE),
    RateLimit(limit_id=WS_ENDPOINTS_LIMIT_ID, limit=WS_LIMIT, time_interval=MINUTE)
]
